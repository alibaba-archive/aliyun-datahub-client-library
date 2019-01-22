package com.aliyun.datahub.clientlibrary.common;

import com.aliyun.datahub.client.common.DatahubConfig;

import java.util.concurrent.ConcurrentHashMap;

public abstract class ClientManagerFactory {
    private static final ConcurrentHashMap<String, ClientManager> clientManagerPool = new ConcurrentHashMap<>();

    public static ClientManager getClientManager(String projectName, String topicName, DatahubConfig datahubConfig) {
        String key = ClientManager.genKey(datahubConfig.getEndpoint(), projectName, topicName);
        if (!clientManagerPool.containsKey(key)) {
            synchronized (clientManagerPool) {
                if (!clientManagerPool.containsKey(key)) {
                    clientManagerPool.put(key, new ClientManager(projectName, topicName, datahubConfig));
                }
            }
        }
        ClientManager clientManager = clientManagerPool.get(key);
        clientManager.addRefCount();
        return clientManager;
    }

    public static boolean removeClientManager(String key) {
        synchronized (clientManagerPool) {
            ClientManager clientManager = clientManagerPool.get(key);
            if (clientManager != null && clientManager.getRefCount() <= 0) {
                clientManagerPool.remove(key);
                return true;
            }
        }
        return false;
    }
}
