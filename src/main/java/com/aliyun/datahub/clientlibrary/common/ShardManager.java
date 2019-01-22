package com.aliyun.datahub.clientlibrary.common;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.clientlibrary.models.ShardMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShardManager {
    private static final Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    private String projectName;
    private String topicName;

    private DatahubClient client;
    private volatile ShardMeta shardMeta;
    private final Object updateCond = new Object();
    private volatile boolean closed = false;
    private final AtomicBoolean threadRunning = new AtomicBoolean(false);

    public ShardManager(String projectName, String topicName, DatahubClient client) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.client = client;
        this.shardMeta = new ShardMeta(client.listShard(projectName, topicName));
        startRefreshThread();
    }

    public void triggerUpdate() {
        synchronized (updateCond) {
            updateCond.notifyAll();
        }
    }

    public ShardMeta getShardMeta() {
        return shardMeta;
    }

    public void close() {
        closed = true;
        triggerUpdate();
    }

    private void waitSignal() throws InterruptedException {
        synchronized (updateCond) {
            if (!closed) {
                updateCond.wait(Constants.UPDATE_SHARD_META_INTERVAL_MS);
            }
        }
    }

    private boolean updateShardMeta() {
        try {
            shardMeta = new ShardMeta(client.listShard(projectName, topicName));
            LOG.debug("Update shard meta, Project: {}, Topic: {}, Shards: {}",
                    projectName, topicName, shardMeta.getActiveShardIds().toString());
            return shardMeta.isFinished();
        } catch (DatahubClientException e) {
            LOG.warn("Update shard meta failed, Project: {}, Topic: {}, Exception: {}",
                    projectName, topicName , e.getMessage());
        }

        return false;
    }

    private void startRefreshThread() {
        if (!threadRunning.compareAndSet(false, true)) {
            return;
        }
        Thread refreshThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!closed) {
                        for (int retry = 0; retry <= Constants.RETRY_TIMES; ++retry) {
                            boolean finished = updateShardMeta();
                            if (finished) {
                                break;
                            }
                            // sleep at least 1 second, for shard not ready
                            try {
                                Thread.sleep(Constants.RETRY_INTERVAL_MS);
                            } catch (InterruptedException e) {
                                LOG.warn(e.getMessage());
                            }
                        }
                        waitSignal();
                    }
                } catch (Throwable ignored) {
                } finally {
                    threadRunning.set(false);
                }
            }
        });
        refreshThread.setDaemon(true);
        refreshThread.start();
    }
}
