package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.SubscriptionOffsetResetException;
import com.aliyun.datahub.client.model.HeartbeatResult;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.exception.ExceptionRetryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class Heartbeat {
    private static final Logger LOG = LoggerFactory.getLogger(Heartbeat.class);

    private String projectName;
    private String topicName;
    private String subId;
    private String consumerId = "";

    private final Set<String> readEndShards = new HashSet<>();
    private volatile Set<String> shards = new HashSet<>();
    private volatile long planVersion = Constants.DEFAULT_PLAN_VERSION;

    private DatahubClient client;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);

    Heartbeat(DatahubClient client, String projectName, String topicName, String subId) {
        this.client = client;
        this.projectName = projectName;
        this.topicName = topicName;
        this.subId = subId;
    }

    void updateReadEndShardList(List<String> readEndShardList) {
        if (!checkRunning() || readEndShardList.isEmpty()) {
            return;
        }

        synchronized (readEndShards) {
            readEndShards.addAll(readEndShardList);
        }
    }

    synchronized void start(String consumerId, long versionId, long heartbeatIntervalMs) {
        this.consumerId = consumerId;
        this.planVersion = Constants.DEFAULT_PLAN_VERSION;

        heartbeatTask = executor.scheduleWithFixedDelay(new HeartBeatRunnable(projectName, topicName, subId, consumerId, versionId),
                0L, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Heartbeat start, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, IntervalMs: {}",
                projectName, topicName, subId, consumerId, heartbeatIntervalMs);
    }

    void stop() {
        if (heartbeatTask == null) {
            return;
        }
        heartbeatTask.cancel(true);
        heartbeatTask = null;

        shards.clear();
        readEndShards.clear();

        LOG.info("Heartbeat stop, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}",
                projectName, topicName, subId, consumerId);
    }

    void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    Set<String> getShards() {
        return shards;
    }

    long getPlanVersion() {
        return planVersion;
    }

    boolean checkRunning() {
        if (heartbeatTask == null) {
            return false;
        }

        if (heartbeatTask.isDone()) {
            try {
                heartbeatTask.get();
                return true;
            } catch (CancellationException | InterruptedException ignored) {
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SubscriptionOffsetResetException) {
                    throw (SubscriptionOffsetResetException) e.getCause();
                }
            }
            return false;
        }
        return true;
    }

    private void cleanReadEndShards() {
        synchronized (readEndShards) {
            readEndShards.retainAll(shards);
        }
    }

    private class HeartBeatRunnable implements Runnable {
        private String projectName;
        private String topicName;
        private String subId;
        private String consumerId;
        private long versionId;

        HeartBeatRunnable(String projectName, String topicName, String subId, String consumerId, long versionId) {
            this.projectName = projectName;
            this.topicName = topicName;
            this.subId = subId;
            this.consumerId = consumerId;
            this.versionId = versionId;
        }

        void heartbeat() {
            new ExceptionRetryer<Void>() {
                @Override
                protected Void func() {
                    List<String> currentShardList = new ArrayList<>(shards);
                    List<String> readEndShardList = Collections.emptyList();
                    if (!readEndShards.isEmpty()) {
                        LOG.info("Upload read end shards, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, ReadEndShardList: {}",
                                projectName, topicName, subId, consumerId, readEndShards.toString());
                        synchronized (readEndShards) {
                            readEndShardList = new ArrayList<>(readEndShards);
                        }
                    }

                    HeartbeatResult heartbeatResult = client.heartbeat(projectName, topicName,
                            subId, consumerId, versionId, currentShardList, readEndShardList);
                    planVersion = heartbeatResult.getPlanVersion();
                    Set<String> newShards = new HashSet<>(heartbeatResult.getShardList());
                    if (newShards.equals(shards)) {
                        // shard list not change
                        return null;
                    }

                    LOG.info("Heartbeat success, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Version: {}, OldShardList: {}, NewShardList: {}",
                            projectName, topicName, subId, consumerId, planVersion, shards.toString(), newShards.toString());
                    shards = newShards;
                    cleanReadEndShards();
                    return null;
                }

                @Override
                protected void failLog(String message) {
                }
            }.run(Constants.RETRY_TIMES, Constants.RETRY_INTERVAL_MS);
        }

        @Override
        public void run() {
            try {
                heartbeat();
            } catch (SubscriptionOffsetResetException e) {
                LOG.error("Heartbeat failed, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                        projectName, topicName, subId, consumerId, e.getMessage());
                throw e;
            } catch (DatahubClientException e) {
                LOG.warn("Heartbeat failed, try rejoin, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                        projectName, topicName, subId, consumerId, e.getMessage());
                throw e;
            } catch (Throwable e) {
                LOG.warn("Heartbeat failed, try rejoin, Project: {}, Topic: {}, SubId: {}, ConsumerId: {}, Exception: {}",
                        projectName, topicName, subId, consumerId, e.getMessage());
                throw new DatahubClientException(e.getMessage());
            }
        }
    }
}
