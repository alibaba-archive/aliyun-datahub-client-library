package com.aliyun.datahub.clientlibrary.common;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientManager {
    private DatahubConfig config;
    private String projectName;
    private String topicName;
    private String key;
    private DatahubClientBuilder builder;
    private DatahubClient defaultClient;
    private ShardManager shardManager;

    private final AtomicInteger refCount = new AtomicInteger(0);
    private final ConcurrentHashMap<String, DatahubClient> clientMap = new ConcurrentHashMap<>();

    public ClientManager(String projectName, String topicName, DatahubConfig config) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.key = genKey(config.getEndpoint(), projectName, topicName);
        this.config = config;

        this.builder = DatahubClientBuilder.newBuilder().setDatahubConfig(config);
        this.defaultClient = builder.build();
        this.shardManager = new ShardManager(projectName, topicName, defaultClient);
    }

    public DatahubClient getClient() {
        return defaultClient;
    }

    public DatahubClient getClient(String shardId) {
        String address = shardManager.getShardMeta().getAddressMap().get(shardId);

        if (address == null || address.isEmpty()) {
            return defaultClient;
        }

        if (!clientMap.containsKey(shardId)) {
            synchronized (clientMap) {
                if (!clientMap.containsKey(shardId)) {
                    clientMap.put(shardId, builder.build());
                }
            }
        }

        DatahubClient clientForShard = clientMap.get(shardId);
        clientForShard.setEndpoint(address);
        return clientForShard;
    }

    public ShardManager getShardManager() {
        return shardManager;
    }

    public void addRefCount() {
        refCount.incrementAndGet();
    }

    public int getRefCount() {
        return refCount.get();
    }

    public void close() {
        if (refCount.decrementAndGet() <= 0) {
            if (ClientManagerFactory.removeClientManager(key)) {
                shardManager.close();
            }
        }
    }

    public static String genKey(String endpoint, String projectName, String topicName) {
        return endpoint + "@" + projectName + "@" + topicName;
    }
}
