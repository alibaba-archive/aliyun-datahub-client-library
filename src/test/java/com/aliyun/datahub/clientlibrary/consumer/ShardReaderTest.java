package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.MockServer;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;
import com.aliyun.datahub.clientlibrary.models.TopicInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ShardReaderTest extends MockServer {

    private void fetchUntilNull(ShardReader shardReader) {
        for (int i = 0; i < 100; ++i) {
            if (shardReader.read() != null) {
                break;
            }
            sleep(1000);
        }

        for (int i = 0; i < 100; ++i) {
            if (shardReader.read() == null) {
                break;
            }
        }
    }

    private RecordEntry fetchResult(ShardReader shardReader) {
        while (true) {
            RecordEntry result = shardReader.read();
            if (result != null) {
                return result;
            }
            sleep(1000);
        }
    }

    ShardReader getDefaultFetcher(ExecutorService executor) {
        return new ShardReader(new TopicInfo("test_project", "test_topic", genRecordSchema()),
                "0", new Offset(1, 1), new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token"), executor);
    }

    @Test
    public void testFetchedRecords() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockSuccess(shardExpectation("sub", -1), GET_RECORD_RESULT);

        ShardReader shardReader = getDefaultFetcher(executor);

        RecordEntry result = fetchResult(shardReader);

        Assert.assertNotNull(result);
        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(1));
        shardReader.close();
        executor.shutdownNow();
    }

    @Test
    public void testReadEnd() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 400, SHARD_SEALED);

        ShardReader shardReader = getDefaultFetcher(executor);

        fetchUntilNull(shardReader);

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(1));
        Assert.assertTrue(shardReader.isReadEnd());
        shardReader.close();
        executor.shutdownNow();
    }

    @Test
    public void testInvalidShard() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 400, NO_SUCH_SHARD);

        ShardReader shardReader = getDefaultFetcher(executor);

        try {
            fetchUntilNull(shardReader);
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("ShardId Not Exist. Invalid shard id", e.getErrorMessage());
        }

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(1));
        Assert.assertFalse(shardReader.isReadEnd());
        executor.shutdownNow();
        shardReader.close();
    }

    @Test
    public void testRetryLimitExceeded() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 500, INTERNAL_SERVER_ERROR);

        ShardReader shardReader = getDefaultFetcher(executor);

        try {
            fetchUntilNull(shardReader);
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Service internal error, please try again later.", e.getErrorMessage());
        }

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(2));
        executor.shutdownNow();
        shardReader.close();
    }

    @Test
    public void testCursorExpired() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockSuccess(cursorExpectation("SYSTEM_TIME", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", 1), 400, INVALID_CURSOR);
        mockSuccess(shardExpectation("sub", -1), GET_RECORD_RESULT);

        ShardReader shardReader = getDefaultFetcher(executor);
        RecordEntry result = fetchResult(shardReader);

        Assert.assertNotNull(result);
        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.exactly(1));
        mockServerClient.verify(cursorRequest("SYSTEM_TIME"), VerificationTimes.exactly(1));
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(2));

        shardReader.close();
        executor.shutdownNow();
    }

    @Test
    public void testReSeekLimitExceeded() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockSuccess(cursorExpectation("SYSTEM_TIME", -1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 400, INVALID_CURSOR);

        ShardReader shardReader = getDefaultFetcher(executor);
        for (int i = 0; i < 10; ++i) {
            shardReader.read();
            sleep(1000);
        }

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(cursorRequest("SYSTEM_TIME"), VerificationTimes.exactly(10));
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.exactly(10));

        shardReader.close();
        executor.shutdownNow();
    }

    @Test
    public void testShardReload() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 400, NO_SUCH_SHARD);

        ShardReader shardReader = getDefaultFetcher(executor);

        try {
            for (int i = 0; i < 20; ++i) {
                shardReader.read();
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("ShardId Not Exist. Invalid shard id", e.getErrorMessage());
        }

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(15));

        shardReader.close();
        executor.shutdownNow();
    }

    @Test
    public void testReadAfterException() {
        ExecutorService executor = Executors.newCachedThreadPool();

        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 1), GET_CURSOR_RESULT);
        mockFail(shardExpectation("sub", -1), 400, INTERNAL_SERVER_ERROR);

        ShardReader shardReader = getDefaultFetcher(executor);

        for (int j = 0; j < 2; ++j) {
            try {
                for (int i = 0; i < 20; ++i) {
                    shardReader.read();
                    sleep(1000);
                }
                Assert.fail("throw exception");
            } catch (DatahubClientException e) {
                Assert.assertEquals("Service internal error, please try again later.", e.getErrorMessage());
            }
        }

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.once());
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(15));

        shardReader.close();
        executor.shutdownNow();
    }
}
