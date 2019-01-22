package com.aliyun.datahub.clientlibrary.config;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.clientlibrary.interceptor.RecordInterceptor;

public class ConsumerConfig extends BaseConfig {
    private static final long MIN_OFFSET_COMMIT_TIMEOUT_MS = 10000;
    private static final long MAX_OFFSET_COMMIT_TIMEOUT_MS = 300000;
    private static final long DEFAULT_OFFSET_COMMIT_TIMEOUT_MS = 30000;

    private static final long MIN_CONSUMER_SESSION_TIMEOUT_MS = 60000;
    private static final long MAX_CONSUMER_SESSION_TIMEOUT_MS = 180000;
    private static final int DEFAULT_FETCH_SIZE = 1000;

    private boolean autoCommit = true;
    private long offsetCommitTimeoutMs = DEFAULT_OFFSET_COMMIT_TIMEOUT_MS;
    private long sessionTimeoutMs = MIN_CONSUMER_SESSION_TIMEOUT_MS;
    private int fetchSize = DEFAULT_FETCH_SIZE;

    public ConsumerConfig(String endpoint, String accessId, String accessKey) {
        super(endpoint, new AliyunAccount(accessId, accessKey));
    }

    public ConsumerConfig(String endpoint, String accessId, String accessKey, String securityToken) {
        super(endpoint, new AliyunAccount(accessId, accessKey, securityToken));
    }

    protected ConsumerConfig(String endpoint, Account account, RecordInterceptor interceptor) {
        super(endpoint, account, interceptor);
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public long getOffsetCommitTimeoutMs() {
        return offsetCommitTimeoutMs;
    }

    public void setOffsetCommitTimeoutMs(long offsetCommitTimeoutMs) {
        if (offsetCommitTimeoutMs < MIN_OFFSET_COMMIT_TIMEOUT_MS) {
            this.offsetCommitTimeoutMs = MIN_OFFSET_COMMIT_TIMEOUT_MS;
        } else if (offsetCommitTimeoutMs > MAX_OFFSET_COMMIT_TIMEOUT_MS) {
            this.offsetCommitTimeoutMs = MAX_OFFSET_COMMIT_TIMEOUT_MS;
        } else {
            this.offsetCommitTimeoutMs = offsetCommitTimeoutMs;
        }
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public long getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(long sessionTimeoutMs) {
        if (sessionTimeoutMs < MIN_CONSUMER_SESSION_TIMEOUT_MS) {
            this.sessionTimeoutMs = MIN_CONSUMER_SESSION_TIMEOUT_MS;
        } else if (sessionTimeoutMs > MAX_CONSUMER_SESSION_TIMEOUT_MS) {
            this.sessionTimeoutMs = MAX_CONSUMER_SESSION_TIMEOUT_MS;
        } else {
            this.sessionTimeoutMs = sessionTimeoutMs;
        }
    }
}
