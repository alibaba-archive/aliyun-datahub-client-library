package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.SubscriptionOffsetResetException;
import com.aliyun.datahub.client.model.JoinGroupResult;
import com.aliyun.datahub.clientlibrary.common.ClientManager;
import com.aliyun.datahub.clientlibrary.common.ClientManagerFactory;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.exception.ExceptionRetryer;
import com.aliyun.datahub.clientlibrary.models.Assignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShardCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardCoordinator.class);

    private String projectName;
    private String topicName;
    private String subId;
    private String consumerId;
    private long versionId;

    private ConsumerConfig config;
    private ClientManager clientManager;
    private DatahubClient client;

    private Heartbeat heartbeat;
    private ExecutorService executor;
    private Set<String> currentAssignment = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ShardCoordinator(String projectName, String topicName, String subId, ConsumerConfig config) {
        this.config = config;
        this.projectName = projectName;
        this.topicName = topicName;
        this.subId = subId;

        this.clientManager = ClientManagerFactory.getClientManager(projectName, topicName, config.getDatahubConfig());
        this.client = clientManager.getClient();
        this.heartbeat = new Heartbeat(client, projectName, topicName, subId);
        this.executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
        init();
    }

    void syncGroup(List<String> releaseShardList, List<String> readEndShardList) {
        if (!heartbeat.checkRunning()) {
            return;
        }

        if (releaseShardList.isEmpty() && readEndShardList.isEmpty()) {
            return;
        }

        heartbeat.updateReadEndShardList(readEndShardList);

        try {
            client.syncGroup(projectName, topicName, subId, consumerId, versionId, releaseShardList, readEndShardList);
            LOG.info("Sync group success, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, ReleaseShardList: {}, ReadEndShardList: {}",
                    projectName, topicName, subId, consumerId, releaseShardList.toString(), readEndShardList.toString());
        } catch (ResourceNotFoundException e) {
            LOG.warn("Stop heartbeat and rejoin group, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                    projectName, topicName, subId, consumerId, e.getMessage());
            heartbeat.stop();
        } catch (SubscriptionOffsetResetException e) {
            LOG.warn("Sync group failed, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                    projectName, topicName, subId, consumerId, e.getMessage());
            throw e;
        } catch (DatahubClientException e) {
            LOG.warn("Sync group failed, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                    projectName, topicName, subId, consumerId, e.getMessage());
        }
    }

    void rejoinIfNeeded() {
        if (heartbeat.checkRunning()) {
            return;
        }

        joinGroup();
    }

    void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                client.leaveGroup(projectName, topicName, subId, consumerId, versionId);
            } catch (Throwable ignored) {
            }
            if (heartbeat != null) {
                heartbeat.close();
            }
            if (executor != null) {
                executor.shutdownNow();
            }
            if (clientManager != null) {
                clientManager.close();
            }
        }
    }

    Assignment getNewAssignment() {
        Set<String> newAssignment = heartbeat.getShards();
        if (newAssignment == currentAssignment) {
            // is the same object
            return Assignment.emptyAssignment;
        }

        Assignment result = new Assignment();

        // find release shard
        for (String shardId : currentAssignment) {
            if (!newAssignment.contains(shardId)) {
                result.getReleaseShardList().add(shardId);
            }
        }

        // find new shard
        for (String shardId : newAssignment) {
            if (!currentAssignment.contains(shardId)) {
                result.getNewShardList().add(shardId);
            }
        }

        currentAssignment = newAssignment;
        return result;
    }

    private void joinGroup() {
        new ExceptionRetryer<Void>() {
            @Override
            protected Void func() {
                JoinGroupResult joinGroupResult = client.joinGroup(projectName, topicName, subId, config.getSessionTimeoutMs());
                consumerId = joinGroupResult.getConsumerId();
                versionId = joinGroupResult.getVersionId();
                long heartbeatIntervalMs = (long) (joinGroupResult.getSessionTimeout() * Constants.HEARTBEAT_INTERVAL_SCALE);
                heartbeat.start(consumerId, versionId, heartbeatIntervalMs);
                LOG.info("Join group success, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, VersionId: {}, SessionTimeout: {}",
                        projectName, topicName, subId, consumerId, versionId, joinGroupResult.getSessionTimeout());
                return null;
            }

            @Override
            protected void failLog(String message) {
                LOG.error("Join group failed, Project: {}, Topic: {}, SubId: {}, Exception: {}",
                        projectName, topicName, subId, message);
            }
        }.run(Constants.RETRY_TIMES, Constants.RETRY_INTERVAL_MS);
    }

    private void init() {
        try {
            joinGroup();
        } catch (Exception e) {
            close();
            throw e;
        }
    }
}
