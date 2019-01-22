package com.aliyun.datahub.clientlibrary.exception;

import com.aliyun.datahub.client.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class ExceptionRetryer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExceptionRetryer.class);
    private static List<Class<? extends DatahubClientException>> EXCEPTIONS_TYPES = new ArrayList<>();

    static {
        EXCEPTIONS_TYPES.add(InvalidParameterException.class);
        EXCEPTIONS_TYPES.add(ShardSealedException.class);
        EXCEPTIONS_TYPES.add(SubscriptionOffsetResetException.class);
        EXCEPTIONS_TYPES.add(SubscriptionOfflineException.class);
        EXCEPTIONS_TYPES.add(SubscriptionSessionInvalidException.class);
    }

    protected abstract T func();

    protected abstract void failLog(String message);

    public final T run(int retryTimes, long intervalMs) {
        for (int i = 0; i <= retryTimes; ++i) {
            try {
                return func();
            } catch (DatahubClientException e) {
                if (i == retryTimes) {
                    failLog(e.getMessage());
                }

                if (i == retryTimes || EXCEPTIONS_TYPES.contains(e.getClass())) {
                    throw e;
                }

                LOG.warn("Request failed, sleep and retry, Exception: {}", e.getMessage());

                if (intervalMs > 0) {
                    try {
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e1) {
                        LOG.warn(e.getMessage());
                    }
                }
            }
        }

        // never reach here
        throw new DatahubClientException("Unknown exception");
    }
}
