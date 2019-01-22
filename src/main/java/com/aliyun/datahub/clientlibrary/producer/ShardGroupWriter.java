package com.aliyun.datahub.clientlibrary.producer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShardGroupWriter {
    private ProducerConfig config;
    private String projectName;
    private String topicName;

    private final ShardWriterPicker shardWriterPicker = new ShardWriterPicker();
    private final Map<String, ShardWriter> shardWriterMap = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ShardGroupWriter(String projectName, String topicName, ProducerConfig config) {
        this.config = config;
        this.projectName = projectName;
        this.topicName = topicName;
    }

    void createShardWriter(List<String> shardIds) {
        checkNotClosed();
        for (String shardId : shardIds) {
            if (shardWriterMap.containsKey(shardId)) {
                continue;
            }
            ShardWriter shardWriter = new ShardWriter(projectName, topicName, shardId, config);
            shardWriterMap.put(shardId, shardWriter);
            shardWriterPicker.addShardWriter(shardWriter);
        }
    }

    void removeShardWriter(List<String> shardIds) {
        checkNotClosed();
        for (String shardId : shardIds) {
            ShardWriter shardWriter = shardWriterMap.get(shardId);
            if (shardWriter != null) {
                shardWriter.close();
                shardWriterMap.remove(shardId);
                shardWriterPicker.removeShardWriter(shardWriter);
            }
        }
    }

    void write(List<RecordEntry> records) {
        checkNotClosed();
        ShardWriter shardWriter = shardWriterPicker.pick();

        if (shardWriter == null) {
            throw new DatahubClientException("No active shard");
        }

        try {
            shardWriter.write(records);
        } catch (ShardSealedException e) {
            shardWriterPicker.removeShardWriter(shardWriter);
            throw e;
        }
    }

    void close() {
        if (closed.compareAndSet(false, true)) {
            for (ShardWriter shardWriter : shardWriterMap.values()) {
                shardWriter.close();
                shardWriterPicker.removeShardWriter(shardWriter);
            }
            shardWriterMap.clear();
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new DatahubClientException("This shard group writer has already been closed");
        }
    }

    private class ShardWriterPicker {
        private int index = 0;
        private final List<ShardWriter> shardWriterList = new ArrayList<>();

        ShardWriter pick() {
            if (shardWriterList.isEmpty()) {
                return null;
            }
            index = (index + 1) % shardWriterList.size();
            return shardWriterList.get(index);
        }

        void addShardWriter(ShardWriter shardWriter) {
            shardWriterList.add(shardWriter);
        }

        void removeShardWriter(ShardWriter shardWriter) {
            shardWriterList.remove(shardWriter);
        }
    }
}
