package com.aliyun.datahub.clientlibrary.producer;

import com.aliyun.datahub.clientlibrary.common.ClientManager;
import com.aliyun.datahub.clientlibrary.common.ClientManagerFactory;
import com.aliyun.datahub.clientlibrary.common.ShardManager;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.models.Assignment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShardAssigner {
    private ClientManager clientManager;
    private ShardManager shardManager;

    private Set<String> currentAssignment = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ShardAssigner(String projectName, String topicName, ProducerConfig config) {
        clientManager = ClientManagerFactory.getClientManager(projectName, topicName, config.getDatahubConfig());
        shardManager = clientManager.getShardManager();
    }

    boolean checkAllActive(List<String> shardIds) {
        return shardManager.getShardMeta().getActiveShardIds().containsAll(shardIds);
    }

    void triggerUpdate() {
        shardManager.triggerUpdate();
    }

    Assignment getNewAssignment() {
        Set<String> newAssignment = shardManager.getShardMeta().getActiveShardIds();
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

    void close() {
        if (closed.compareAndSet(false, true)) {
            clientManager.close();
        }
    }
}
