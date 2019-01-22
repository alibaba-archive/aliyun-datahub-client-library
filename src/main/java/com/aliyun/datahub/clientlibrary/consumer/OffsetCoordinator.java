package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.clientlibrary.common.ClientManager;
import com.aliyun.datahub.clientlibrary.common.ClientManagerFactory;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.exception.ExceptionRetryer;
import com.aliyun.datahub.clientlibrary.models.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class OffsetCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetCoordinator.class);

    private String projectName;
    private String topicName;
    private String subId;

    private ConsumerConfig config;
    private ClientManager clientManager;
    private DatahubClient client;

    private volatile long lastCommitTime = System.currentTimeMillis();
    private Map<String, SubscriptionOffset> readOffsets = new HashMap<>();
    private volatile Map<String, Offset> committedOffsets = new HashMap<>();

    private Future commitTask;
    private ExecutorService executor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());

    private volatile DatahubClientException exception;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    OffsetCoordinator(String projectName, String topicName, String subId, ConsumerConfig consumerConfig) {
        this.config = consumerConfig;
        this.projectName = projectName;
        this.topicName = topicName;
        this.subId = subId;

        this.clientManager = ClientManagerFactory.getClientManager(projectName, topicName, config.getDatahubConfig());
        this.client = clientManager.getClient();
    }

    void commitIfNeeded() {
        if (!config.isAutoCommit()) {
            return;
        }

        if (exception != null) {
            throw exception;
        }

        long now = System.currentTimeMillis();
        if (now - lastCommitTime > config.getOffsetCommitTimeoutMs()) {
            commitAsync(readOffsets, false);
        }
    }

    void setOffset(String shardId, long sequence, long timestamp) {
        SubscriptionOffset offset = readOffsets.get(shardId);
        if (offset == null) {
            // should never reach here in single thread
            throw new DatahubClientException("Invalid status, offset not match shard assigned");
        }

        offset.setSequence(sequence);
        offset.setTimestamp(timestamp);
    }

    Map<String, Offset> openAndGetOffsets(List<String> shards) {
        List<String> shardIds = new ArrayList<>();
        Map<String, Offset> result = new HashMap<>();

        for (String shardId : shards) {
            if (!readOffsets.containsKey(shardId)) {
                shardIds.add(shardId);
            }
        }

        if (shardIds.isEmpty()) {
            return result;
        }

        readOffsets.putAll(openSubscriptionSession(shardIds));

        for (String shardId : shards) {
            SubscriptionOffset subscriptionOffset = readOffsets.get(shardId);
            result.put(shardId, getReadOffset(subscriptionOffset));
        }

        return result;
    }

    void removeOffsets(List<String> shards) {
        if (!shards.isEmpty()) {
            // trigger commit async before remove
            // ignore the exception during shard transfer
            commitAsync(readOffsets, true);
        }

        for (String shardId : shards) {
            readOffsets.remove(shardId);
        }
    }

    List<String> getReadEndShardList(Map<String, Long> endSequenceMap) {
        // get read end and committed shard list
        if (endSequenceMap.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> readEndShardList = new ArrayList<>();
        for (String shardId : endSequenceMap.keySet()) {
            if (!readOffsets.containsKey(shardId)) {
                continue;
            }
            if (committedOffsets.containsKey(shardId) &&
                    committedOffsets.get(shardId).getSequence() == endSequenceMap.get(shardId)) {
                readEndShardList.add(shardId);
                committedOffsets.remove(shardId);
            } else {
                // read end but not committed yet, so trigger async commit
                commitAsync(readOffsets, false);
            }
        }
        return readEndShardList;
    }

    void close() {
        if (closed.compareAndSet(false, true)) {
            if (clientManager != null) {
                clientManager.close();
            }
        }
    }

    private Map<String, SubscriptionOffset> openSubscriptionSession(final List<String> shardIds) {
        return new ExceptionRetryer<Map<String, SubscriptionOffset>>() {
            @Override
            protected Map<String, SubscriptionOffset> func() {
                return client.openSubscriptionSession(projectName, topicName, subId, shardIds).getOffsets();
            }

            @Override
            protected void failLog(String message) {
                LOG.error("Init offset failed, Project: {}, Topic: {}, SubId: {}, Exception: {}",
                        projectName, topicName, subId, message);
            }
        }.run(Constants.RETRY_TIMES, Constants.RETRY_INTERVAL_MS);
    }

    private void commitAsync(Map<String, SubscriptionOffset> offsets, boolean ignoreException) {
        if (offsets.isEmpty() || (commitTask != null && !commitTask.isDone())) {
            return;
        }

        Map<String, SubscriptionOffset> offsetsToCommit = new HashMap<>(offsets);
        commitTask = executor.submit(new CommitRunnable(offsetsToCommit, ignoreException));
    }

    private void commit(final Map<String, SubscriptionOffset> offsets) {
        new ExceptionRetryer<Void>() {
            @Override
            protected Void func() {
                client.commitSubscriptionOffset(projectName, topicName, subId, offsets);
                LOG.debug("Commit offset success, Project: {}, Topic: {}, SubId: {}",
                        projectName, topicName, subId);
                Map<String, Offset> newCommittedOffset = new HashMap<>();
                for (Map.Entry<String, SubscriptionOffset> entry : offsets.entrySet()) {
                    newCommittedOffset.put(entry.getKey(), new Offset(entry.getValue()));
                }
                lastCommitTime = System.currentTimeMillis();
                committedOffsets = newCommittedOffset;
                return null;
            }

            @Override
            protected void failLog(String message) {
                LOG.error("Commit offset failed, Project: {}, Topic: {}, SubId: {}, Exception: {}",
                        projectName, topicName, subId, message);
            }
        }.run(Constants.RETRY_TIMES, Constants.RETRY_INTERVAL_MS);
    }

    private class CommitRunnable implements Runnable {
        private Map<String, SubscriptionOffset> offsets;
        private boolean ignoreException;

        CommitRunnable(Map<String, SubscriptionOffset> offsets, boolean ignoreException) {
            this.offsets = offsets;
            this.ignoreException = ignoreException;
        }

        @Override
        public void run() {
            DatahubClientException ex = null;
            try {
                commit(offsets);
            } catch (DatahubClientException e) {
                ex = e;
            } catch (Throwable e) {
                ex = new DatahubClientException(e.getMessage());
            } finally {
                if (!ignoreException) {
                    exception = ex;
                }
            }
        }
    }

    private Offset getReadOffset(SubscriptionOffset subscriptionOffset) {
        // get next sequence to read if committed before
        long sequence = subscriptionOffset.getSequence();
        long timestamp = subscriptionOffset.getTimestamp();
        // the initial sequence is expected to be -1
        if (timestamp <= 0) {
            timestamp = 0;
            sequence = 0;
        } else {
            ++sequence;
        }
        return new Offset(sequence, timestamp);
    }
}
