package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.util.FormatUtils;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Assignment;
import com.aliyun.datahub.clientlibrary.models.Offset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Not thread safe
 */
public class Consumer {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private OffsetCoordinator offsetCoordinator;
    private ShardGroupReader shardGroupReader;
    private ShardCoordinator shardCoordinator;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Mode                  :  Collaborative Consumption
     * Auto assign shards    :  Y
     * Use offset service    :  Y
     * Perceive offset reset :  Throw exception within commit interval
     *
     * @param projectName The name of the project.
     * @param topicName   The name of the topic.
     * @param subId       The subscription id.
     * @param config      The consumer config
     */
    public Consumer(String projectName, String topicName, String subId, ConsumerConfig config) {
        preCheck(projectName, topicName, subId);
        try {
            shardGroupReader = new ShardGroupReader(projectName, topicName, config);
            shardGroupReader.setSubId(subId);
            shardCoordinator = new ShardCoordinator(projectName, topicName, subId, config);
            offsetCoordinator = new OffsetCoordinator(projectName, topicName, subId, config);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Mode                  :  Manually assign shards
     * Auto assign shards    :  N
     * Use offset service    :  Y
     * Perceive offset reset :  Throw exception within commit interval
     *
     * @param projectName The name of the project.
     * @param topicName   The name of the topic.
     * @param subId       The subscription id.
     * @param shardIds    The shard ids specified.
     * @param config      The consumer config
     */
    public Consumer(String projectName, String topicName, String subId, List<String> shardIds, ConsumerConfig config) {
        preCheck(projectName, topicName, subId, shardIds);
        try {
            shardGroupReader = new ShardGroupReader(projectName, topicName, config);
            shardGroupReader.setSubId(subId);
            offsetCoordinator = new OffsetCoordinator(projectName, topicName, subId, config);

            shardGroupReader.createShardReader(offsetCoordinator.openAndGetOffsets(shardIds));
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Mode                  :  Manually assign shards and offsets
     * Auto assign shards    :  N
     * Use offset service    :  Only for display (use local offset and just commit to server)
     * Perceive offset reset :  Throw exception within commit interval
     *
     * @param projectName The name of the project.
     * @param topicName   The name of the topic.
     * @param subId       The subscription id.
     * @param offsetMap   The offset map specified.
     * @param config      The consumer config
     */
    public Consumer(String projectName, String topicName, String subId, Map<String, Offset> offsetMap, ConsumerConfig config) {
        preCheck(projectName, topicName, subId, offsetMap);
        try {
            shardGroupReader = new ShardGroupReader(projectName, topicName, config);
            shardGroupReader.setSubId(subId);
            offsetCoordinator = new OffsetCoordinator(projectName, topicName, subId, config);

            shardGroupReader.createShardReader(offsetMap);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Read record
     *
     * @param maxRetry The max retry times.
     * @return One record or null if not fetched
     */
    public RecordEntry read(int maxRetry) {
        if (closed.get()) {
            throw new InvalidParameterException("This consumer has already been closed");
        }
        if (maxRetry < 0) {
            throw new InvalidParameterException("Retry must not be negative");
        }

        for (int i = 0; i <= maxRetry && !closed.get(); ++i) {
            syncAssignmentIfNeeded();
            offsetCoordinator.commitIfNeeded();

            RecordEntry record = shardGroupReader.read();
            if (record != null) {
                offsetCoordinator.setOffset(record.getShardId(), record.getSequence(), record.getSystemTime());
                return record;
            }

            try {
                Thread.sleep(Constants.RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }
        return null;
    }

    /**
     * Close the consumer to release resource
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (shardGroupReader != null) {
            shardGroupReader.close();
        }
        if (shardCoordinator != null) {
            shardCoordinator.close();
        }
        if (offsetCoordinator != null) {
            offsetCoordinator.close();
        }
        LOG.info("Consumer closed");
    }

    private void preCheck(String projectName, String topicName, String subId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }
        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }
        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }
    }

    private void preCheck(String projectName, String topicName, String subId, List<String> shardIds) {
        preCheck(projectName, topicName, subId);
        if (shardIds == null || shardIds.isEmpty()) {
            throw new InvalidParameterException("ShardIds must not be empty");
        }
    }

    private void preCheck(String projectName, String topicName, String subId, Map<String, Offset> offsetMap) {
        preCheck(projectName, topicName, subId);
        if (offsetMap == null || offsetMap.isEmpty()) {
            throw new InvalidParameterException("Offset map must not be empty");
        }
    }

    private void syncAssignmentIfNeeded() {
        if (shardCoordinator == null) {
            // manually assign
            return;
        }

        shardCoordinator.rejoinIfNeeded();
        Assignment newAssignment = shardCoordinator.getNewAssignment();

        // release
        offsetCoordinator.removeOffsets(newAssignment.getReleaseShardList());
        shardGroupReader.removeShardReader(newAssignment.getReleaseShardList());

        // create
        Map<String, Offset> newOffsetMap = offsetCoordinator.openAndGetOffsets(newAssignment.getNewShardList());
        shardGroupReader.createShardReader(newOffsetMap);

        // sync
        Map<String, Long> endSequenceMap = shardGroupReader.getEndSequenceMap();
        List<String> readEndShardList = offsetCoordinator.getReadEndShardList(endSequenceMap);
        shardCoordinator.syncGroup(newAssignment.getReleaseShardList(), readEndShardList);
    }
}
