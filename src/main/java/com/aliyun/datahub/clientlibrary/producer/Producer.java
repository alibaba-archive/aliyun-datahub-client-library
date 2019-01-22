package com.aliyun.datahub.clientlibrary.producer;

import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.util.FormatUtils;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.models.Assignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Not thread safe
 */
public class Producer {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private ShardGroupWriter shardGroupWriter;
    private ShardAssigner shardAssigner;
    private boolean autoAssigned;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Producer perceiving shard change
     * @param projectName The name of project.
     * @param topicName The name of topic.
     * @param config The producer config.
     */
    public Producer(String projectName, String topicName, ProducerConfig config) {
        preCheck(projectName, topicName);
        shardGroupWriter = new ShardGroupWriter(projectName, topicName, config);
        shardAssigner = new ShardAssigner(projectName, topicName, config);
        autoAssigned = true;
    }

    /**
     * Producer with shards specified
     * @param projectName The name of project.
     * @param topicName The name of topic.
     * @param shardIds The shard ids.
     * @param config The producer config.
     */
    public Producer(String projectName, String topicName, List<String> shardIds, ProducerConfig config) {
        preCheck(projectName, topicName, shardIds);
        shardAssigner = new ShardAssigner(projectName, topicName, config);
        if (!shardAssigner.checkAllActive(shardIds)) {
            shardAssigner.close();
            throw new InvalidParameterException("Shard must be valid and active");
        }

        shardGroupWriter = new ShardGroupWriter(projectName, topicName, config);
        shardGroupWriter.createShardWriter(shardIds);
        autoAssigned = false;
    }

    /**
     * Send record list
     * @param records The record list to send.
     * @param maxRetry The max retry times.
     */
    public void send(List<RecordEntry> records, int maxRetry) {
        if (closed.get()) {
            throw new DatahubClientException("This producer has already been closed");
        }
        if (maxRetry < 0) {
            throw new InvalidParameterException("Retry must not be negative");
        }

        for (int retry = 0; retry <= maxRetry && !closed.get(); ++retry) {
            try {
                syncAssignmentIfNeeded();
                shardGroupWriter.write(records);
                return;
            } catch (MalformedRecordException | InvalidParameterException e) {
                fail(e, true);
            } catch (ShardSealedException | ResourceNotFoundException e) {
                // split/merge sometimes lead to ResourceNotFoundException because of reloading shard
                fail(e, retry == maxRetry);
                shardAssigner.triggerUpdate();
            } catch (DatahubClientException e) {
                fail(e, retry == maxRetry);
            }
        }
        throw new DatahubClientException("Send records failed, retry limit exceeded");
    }

    /**
     * Close to release resource
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            shardGroupWriter.close();
            if (shardAssigner != null) {
                shardAssigner.close();
            }
        }
    }

    private void preCheck(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }
        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }
    }

    private void preCheck(String projectName, String topicName, List<String> shardIds) {
        preCheck(projectName, topicName);
        if (shardIds == null || shardIds.isEmpty()) {
            shardAssigner.close();
            throw new InvalidParameterException("ShardIds must not be empty");
        }
    }

    private void syncAssignmentIfNeeded() {
        if (!autoAssigned) {
            return;
        }

        Assignment newAssignment = shardAssigner.getNewAssignment();

        shardGroupWriter.createShardWriter(newAssignment.getNewShardList());
        shardGroupWriter.removeShardWriter(newAssignment.getReleaseShardList());
    }

    private void fail(DatahubClientException exception, boolean needThrow) {
        if (needThrow) {
            LOG.error("Send records failed, Exception: {}", exception.getMessage());
            throw exception;
        }
        LOG.warn("Send records failed, will retry, Exception: {}", exception.getMessage());
    }
}
