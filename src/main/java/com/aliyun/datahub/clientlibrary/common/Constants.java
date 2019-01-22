package com.aliyun.datahub.clientlibrary.common;

public class Constants {
    public static final long DEFAULT_LAST_SEQUENCE = -1;
    public static final long DEFAULT_PLAN_VERSION = -1;

    public static final int MIN_FETCH_SIZE = 100;
    public static final int MAX_FETCH_SIZE = 1000;

    public static final int MAX_FETCH_TIMES = 10;
    public static final int MAX_SHARD_READER_POOL_SIZE = 1024;

    public static final int RETRY_TIMES = 3;
    public static final int FETCH_RETRY_TIMES = 15;
    public static final long RETRY_INTERVAL_MS = 1000;

    public static final float HEARTBEAT_INTERVAL_SCALE = 0.66f;
    public static final long UPDATE_SHARD_META_INTERVAL_MS = 300000;
}
