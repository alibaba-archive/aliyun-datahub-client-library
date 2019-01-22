package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyun.datahub.clientlibrary.common.ClientManager;
import com.aliyun.datahub.clientlibrary.common.ClientManagerFactory;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.exception.ExceptionRetryer;
import com.aliyun.datahub.clientlibrary.models.Offset;
import com.aliyun.datahub.clientlibrary.models.TopicInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardReader {
    private static final Logger LOG = LoggerFactory.getLogger(ShardReader.class);

    private ConsumerConfig config;
    private ClientManager clientManager;
    private TopicInfo topicInfo;
    private String shardId;
    private Offset offset;
    private ExecutorService executor;

    private volatile String cursor;
    private volatile Future currentTask;
    private volatile boolean fetchEnd = false;
    private boolean readEnd = false;
    private volatile DatahubClientException exception;
    private volatile long endSequence = Constants.DEFAULT_LAST_SEQUENCE;
    private final ConcurrentLinkedQueue<RecordEntry> fetchedQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // may not match the size of fetchedQueue in real-time, just used to limit fetchIfNeeded frequency
    private final AtomicInteger queueSize = new AtomicInteger(0);

    // only used to display in user agent
    private String subId;

    ShardReader(TopicInfo topicInfo,
                String shardId,
                Offset offset,
                ConsumerConfig config,
                ExecutorService executor) {
        this.topicInfo = topicInfo;
        this.shardId = shardId;
        this.offset = offset;
        this.config = config;
        this.executor = executor;
        this.currentTask = null;
        this.clientManager = ClientManagerFactory.getClientManager(topicInfo.getProjectName(),
                topicInfo.getTopicName(), config.getDatahubConfig());
    }

    String getShardId() {
        return shardId;
    }

    boolean isClosed() {
        return closed.get();
    }

    RecordEntry read() {
        if (exception != null) {
            DatahubClientException ex = exception;
            exception = null;
            throw ex;
        }

        if (closed.get() || readEnd) {
            return null;
        }

        if (fetchEnd) {
            readEnd = fetchedQueue.isEmpty();
        }

        fetchIfNeeded();

        RecordEntry result = fetchedQueue.peek();
        if (result == null) {
            return null;
        }
        fetchedQueue.poll();
        queueSize.decrementAndGet();

        fetchIfNeeded();

        return result;
    }

    boolean isReadEnd() {
        return readEnd;
    }

    long getEndSequence() {
        return endSequence;
    }

    long frontRecordTime() {
        if (fetchedQueue.isEmpty() || fetchedQueue.peek() == null) {
            return Long.MIN_VALUE;
        }
        return fetchedQueue.peek().getSystemTime();
    }

    void close() {
        if (closed.compareAndSet(false, true)) {
            if (clientManager != null) {
                clientManager.close();
            }
            if (currentTask != null && !currentTask.isDone()) {
                currentTask.cancel(true);
            }
        }
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    private void fetchIfNeeded() {
        if (closed.get() || fetchEnd || isTaskRunning()) {
            return;
        }

        // fetch more records with less queue size
        int totalFetchSize = config.getFetchSize() * 2 - queueSize.get();
        if (totalFetchSize <= 0) {
            return;
        }

        FetchTask task = new FetchTask(totalFetchSize);
        try {
            currentTask = executor.submit(task);
        } catch (RejectedExecutionException e) {
            LOG.warn("Thread pool for shard reader is full, Exception: {}", e.getMessage());
        }
    }

    private String seekCursor(final Offset offset) {
        if (offset.isInvalid()) {
            throw new InvalidParameterException("Sequence and system time are all invalid");
        }
        CursorType cursorType = offset.hasSequence() ? CursorType.SEQUENCE : CursorType.SYSTEM_TIME;
        long param = cursorType.equals(CursorType.SEQUENCE) ? offset.getSequence() : offset.getTimestamp();

        try {
            return getCursor(cursorType, param, Constants.RETRY_TIMES);
        } catch (InvalidParameterException e) {
            if (cursorType.equals(CursorType.SYSTEM_TIME) || !offset.hasTimestamp()) {
                throw e;
            }

            LOG.warn("Get cursor by sequence failed, try system time, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                    topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, e.getMessage());
            return getCursor(CursorType.SYSTEM_TIME, offset.getTimestamp(), Constants.RETRY_TIMES);
        }
    }

    private String getCursor(final CursorType cursorType, final long param, int retry) {
        return new ExceptionRetryer<String>() {
            @Override
            protected String func() {
                return clientManager.getClient().getCursor(topicInfo.getProjectName(), topicInfo.getTopicName(),
                        shardId, cursorType, param).getCursor();
            }

            @Override
            protected void failLog(String message) {
                LOG.error("Get cursor with failed, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                        topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, message);
            }
        }.run(retry, Constants.RETRY_INTERVAL_MS);
    }

    private boolean isTaskRunning() {
        if (currentTask != null) {
            if (currentTask.isDone()) {
                try {
                    currentTask.get();
                } catch (CancellationException | InterruptedException ignored) {
                } catch (ExecutionException e) {
                    // other exceptions escape from task catch
                    LOG.error("Fetch task failed, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                            topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, e.getMessage());
                    exception = new DatahubClientException(e.getMessage());
                }
                currentTask = null;
            }
        }
        return currentTask != null;
    }

    private class FetchTask implements Runnable {
        private int totalFetchSize;
        private int fetchSizeOnce;

        FetchTask(int totalFetchSize) {
            this.totalFetchSize = totalFetchSize;
            this.fetchSizeOnce = Math.max(Constants.MIN_FETCH_SIZE, Math.min(totalFetchSize, Constants.MAX_FETCH_SIZE));
        }

        private GetRecordsResult fetchRecords() {
            return new ExceptionRetryer<GetRecordsResult>() {
                @Override
                protected GetRecordsResult func() {
                    DatahubClient client = clientManager.getClient(shardId);
                    client.setUserAgent(subId);
                    if (topicInfo.getRecordType().equals(RecordType.TUPLE)) {
                        return client.getRecords(topicInfo.getProjectName(), topicInfo.getTopicName(),
                                shardId, topicInfo.getRecordSchema(), cursor, fetchSizeOnce);
                    } else {
                        return client.getRecords(topicInfo.getProjectName(), topicInfo.getTopicName(),
                                shardId, cursor, fetchSizeOnce);
                    }
                }

                @Override
                protected void failLog(String message) {
                    LOG.error("Fetch task failed, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                            topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, message);
                }
            }.run(Constants.FETCH_RETRY_TIMES, Constants.RETRY_INTERVAL_MS);
        }

        private void fetch() {
            try {
                if (cursor == null) {
                    cursor = seekCursor(offset);
                }

                int fetchedCount = 0;
                for (int i = 0; i <= Constants.MAX_FETCH_TIMES && fetchedCount < totalFetchSize && !closed.get(); ++i) {
                    GetRecordsResult getRecordsResult = fetchRecords();
                    if (getRecordsResult.getRecordCount() == 0) {
                        break;
                    }

                    RecordEntry lastRecord = getRecordsResult.getRecords().get(getRecordsResult.getRecords().size() - 1);
                    fetchedCount += getRecordsResult.getRecordCount();
                    queueSize.addAndGet(getRecordsResult.getRecordCount());
                    fetchedQueue.addAll(config.getInterceptor().afterRead(getRecordsResult.getRecords()));
                    cursor = getRecordsResult.getNextCursor();
                    endSequence = lastRecord.getSequence();
                }
            } catch (InvalidParameterException e) {
                LOG.warn("Cursor is expired, try seek by offset, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                        topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, e.getMessage());

                if (offset.hasTimestamp()) {
                    cursor = getCursor(CursorType.SYSTEM_TIME, offset.getTimestamp(), 0);
                } else {
                    throw e;
                }
            }
        }

        @Override
        public void run() {
            try {
                fetch();
                exception = null;
            } catch (ShardSealedException e) {
                LOG.warn("Fetch end of shard, Project: {}, Topic: {}, ShardId: {}, Exception: {}",
                        topicInfo.getProjectName(), topicInfo.getTopicName(), shardId, e.getMessage());
                fetchEnd = true;
            } catch (DatahubClientException e) {
                exception = e;
            }
        }
    }
}
