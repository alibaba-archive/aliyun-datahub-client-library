package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.MockServer;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardReaderGroupTest extends MockServer {
    private ShardGroupReader getDefaultFetcherGroup() {
        return new ShardGroupReader("test_project", "test_topic",
                new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token"));
}

    @Test
    public void testRead() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(cursorExpectation("SEQUENCE", 3), GET_CURSOR_RESULT);
        mockSuccess(shardExpectation("sub", -1), GET_RECORD_RESULT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup();

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));

        shardGroupReader.createShardReader(offsetMap);

        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            result.add(shardGroupReader.read());
            if (result.size() > 0) {
                break;
            }
            sleep(1000);
        }

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.exactly(3));
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(3));
        Assert.assertFalse(result.isEmpty());
        shardGroupReader.close();
    }

    @Test
    public void testClosed() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup();
        shardGroupReader.close();
        try {
            shardGroupReader.read();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("This shard group reader has already been closed", e.getErrorMessage());
        }

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        try {
            shardGroupReader.createShardReader(offsetMap);
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("This shard group reader has already been closed", e.getErrorMessage());
        }

        try {
            shardGroupReader.removeShardReader(new ArrayList<>(offsetMap.keySet()));
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("This shard group reader has already been closed", e.getErrorMessage());
        }

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
    }

    @Test
    public void testInvalidSequence() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockFail(cursorExpectation("SEQUENCE", 3), 400, INVALID_SEEK_PARAM);
        mockSuccess(cursorExpectation("SYSTEM_TIME", 3), GET_CURSOR_RESULT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup();

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));
        shardGroupReader.createShardReader(offsetMap);
        shardGroupReader.read();

        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.atLeast(3));
        mockServerClient.verify(cursorRequest("SYSTEM_TIME"), VerificationTimes.atLeast(3));
        shardGroupReader.close();
    }

    @Test
    public void testSeekInvalid() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockFail(cursorExpectation("SEQUENCE", 12), 500, INTERNAL_SERVER_ERROR);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup();

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));
        shardGroupReader.createShardReader(offsetMap);

        try {
            for (int i = 0; i < 10; ++i) {
                shardGroupReader.read();
                sleep(1000);

            }
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Service internal error, please try again later.", e.getErrorMessage());
        }

        mockServerClient.verify(getTopicRequest(), VerificationTimes.once());
        mockServerClient.verify(cursorRequest("SEQUENCE"), VerificationTimes.exactly(12));
        shardGroupReader.close();
    }

    @Test
    public void testTopicException() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockFail(getTopicExpectation(1), 404, NO_SUCH_TOPIC);

        try {
            ShardGroupReader shardGroupReader = getDefaultFetcherGroup();
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("The specified topic name does not exist.", e.getErrorMessage());
        }

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
    }
}
