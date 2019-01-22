package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.common.ClientManager;
import com.aliyun.datahub.clientlibrary.common.ClientManagerFactory;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;
import com.aliyun.datahub.clientlibrary.models.TopicInfo;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Not thread safe
 */
public class ShardGroupReader {
    private TopicInfo topicInfo;

    private ConsumerConfig config;
    private ClientManager clientManager;

    private ShardReaderPicker shardReaderPicker = new ShardReaderPicker();
    private final Map<String, ShardReader> shardReaderMap = new HashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ExecutorService executor = new ThreadPoolExecutor(1, Constants.MAX_SHARD_READER_POOL_SIZE,
            60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    // only used to display in user agent
    private String subId;

    /**
     * Shard group reader can dynamically change the shard plan to read
     * @param projectName The name of project.
     * @param topicName The name of topic.
     * @param config The consumer config.
     */
    public ShardGroupReader(String projectName, String topicName, ConsumerConfig config) {
        this.config = config;
        clientManager = ClientManagerFactory.getClientManager(projectName, topicName, config.getDatahubConfig());
        this.topicInfo = getTopic(projectName, topicName);
    }

    /**
     * Read record
     * @return One record or null if not fetched
     */
    public RecordEntry read() {
        checkNotClosed();

        int maxRetry = shardReaderMap.size();
        for (int retry = 0; retry < maxRetry; ++retry) {
            ShardReader shardReader = shardReaderPicker.pick();

            if (shardReader == null) {
                return null;
            }

            RecordEntry record = shardReader.read();
            if (record != null) {
                return record;
            }
        }
        return null;
    }

    /**
     * Create shard reader
     * @param offsetMap The offset map of shard to create reader
     */
    public void createShardReader(Map<String, Offset> offsetMap) {
        checkNotClosed();

        for (String shardId : offsetMap.keySet()) {
            if (shardReaderMap.containsKey(shardId) && !shardReaderMap.get(shardId).isClosed()) {
                continue;
            }
            ShardReader shardReader = new ShardReader(topicInfo, shardId, offsetMap.get(shardId), config, executor);
            shardReader.setSubId(subId);
            shardReaderMap.put(shardId, shardReader);
        }
    }

    /**
     * Remove shard reader
     * @param shardIds The shard ids of shards to remove
     */
    public void removeShardReader(List<String> shardIds) {
        checkNotClosed();

        for (String shardId : shardIds) {
            ShardReader shardReader = shardReaderMap.get(shardId);
            if (shardReader != null) {
                shardReader.close();
                shardReaderMap.remove(shardId);
            }
        }
    }

    /**
     * Close to release resource
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (ShardReader shardReader : shardReaderMap.values()) {
                shardReader.close();
            }
            shardReaderMap.clear();

            executor.shutdownNow();
            if (clientManager != null) {
                clientManager.close();
            }
        }
    }

    void setSubId(String subId) {
        this.subId = subId;
    }

    Map<String, Long> getEndSequenceMap() {
        // return with endSequence to check if committed
        // only for auto assign mode
        Map<String, Long> result = new HashMap<>();
        for (ShardReader shardReader : shardReaderMap.values()) {
            if (shardReader.isReadEnd()) {
                result.put(shardReader.getShardId(), shardReader.getEndSequence());
            }
        }
        return result;
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new DatahubClientException("This shard group reader has already been closed");
        }
    }

    private TopicInfo getTopic(String projectName, String topicName) {
        try {
            return new TopicInfo(clientManager.getClient().getTopic(projectName, topicName));
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private class ShardReaderPicker {
        private final Set<ShardReader> emptySet = new HashSet<>();

        ShardReader pick() {
            ShardReader result = findOldest();
            if (result != null) {
                if (result.frontRecordTime() == Long.MIN_VALUE) {
                    emptySet.add(result);
                }
                if (emptySet.size() >= shardReaderMap.size() || result.frontRecordTime() != Long.MIN_VALUE) {
                    emptySet.clear();
                }
            }
            return result;
        }

        private ShardReader findOldest() {
            ShardReader result = null;
            for (ShardReader shardReader : shardReaderMap.values()) {
                if (emptySet.contains(shardReader)) {
                    continue;
                }
                if (result == null || shardReader.frontRecordTime() < result.frontRecordTime()) {
                    result = shardReader;
                }
            }
            return result;
        }
    }
}
