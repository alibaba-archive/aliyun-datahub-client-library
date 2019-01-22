package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.SubscriptionOffsetResetException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.MockServer;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ConsumerTest extends MockServer {

    private Consumer getDefaultConsumer() {
        ConsumerConfig consumerConfig = new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token");
        consumerConfig.setOffsetCommitTimeoutMs(10000);
        return new Consumer("test_project", "test_topic", "test_sub_id", consumerConfig);
    }

    private Consumer getConsumerWithShards() {
        ConsumerConfig consumerConfig = new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token");
        return new Consumer("test_project", "test_topic", "test_sub_id", Arrays.asList("0", "1", "2"), consumerConfig);
    }

    private Consumer getConsumerWithOffsets() {
        ConsumerConfig consumerConfig = new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token");
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 10000));
        offsetMap.put("1", new Offset(1, 10000));
        offsetMap.put("2", new Offset(1, 10000));
        return new Consumer("test_project", "test_topic", "test_sub_id", offsetMap, consumerConfig);
    }

    @Test
    public void testCreateAutoAssign() {
        // auto assign with offset management
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 1), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", -1), HEARTBEAT_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);

        Consumer consumer = getDefaultConsumer();
        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.atLeast(1));
        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
        consumer.close();
    }

    @Test
    public void testCreateAssignShards() {
        // manual assign with offset management
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);

        Consumer consumer = getConsumerWithShards();

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());

        consumer.close();
    }

    @Test
    public void testCreateAssignOffsets() {
        // manual assign with offset management
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);

        Consumer consumer = getConsumerWithOffsets();

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));

        consumer.close();
    }

    @Test
    public void testSubscriptionException() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockFail(consumerExpectation("joinGroup", 4), 404, NO_SUCH_SUBSCRIPTION);

        try {
            Consumer consumer = getDefaultConsumer();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Subscription id invalid:", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.exactly(4));
    }

    @Test
    public void testJoinGroupFailed() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockFail(consumerExpectation("joinGroup", 4), 500, INTERNAL_SERVER_ERROR);

        try {
            Consumer consumer = getDefaultConsumer();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Service internal error, please try again later.", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.exactly(4));
    }

    @Test
    public void testTopicException() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockFail(getTopicExpectation(1), 404, NO_SUCH_TOPIC);

        try {
            Consumer consumer = getConsumerWithShards();
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("The specified topic name does not exist.", e.getErrorMessage());
        }

        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
    }

    @Test
    public void testRejoin() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", -1), JOIN_GROUP_RESULT);
        mockFail(consumerExpectation("heartbeat", -1), 400, NO_SUCH_CONSUMER);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);

        Consumer consumer = getDefaultConsumer();
        consumer.read(10);

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.atLeast(2));
        mockServerClient.verify(getTopicRequest(), VerificationTimes.exactly(1));
        consumer.close();
    }


    @Test
    public void testClose() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockSuccess(shardExpectation("cursor", 3), GET_CURSOR_RESULT);
        Consumer consumer = getConsumerWithShards();
        consumer.close();

        try {
            consumer.read(1);
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("This consumer has already been closed", e.getErrorMessage());
        }

        // close more than one times
        consumer.close();
    }

    @Test
    public void testRead() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 1), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", -1), HEARTBEAT_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockSuccess(offsetExpectation("commit", -1), "");
        mockSuccess(shardExpectation("cursor", 3), GET_CURSOR_RESULT);
        mockSuccess(shardExpectation("sub", -1), GET_RECORD_RESULT);

        Consumer consumer = getDefaultConsumer();
        RecordEntry RecordEntry = consumer.read(10);
        Assert.assertNotNull(RecordEntry);

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.atLeast(1));
        mockServerClient.verify(getTopicRequest(), VerificationTimes.once());
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());
        mockServerClient.verify(offsetRequest("commit"), VerificationTimes.atLeast(0));
        mockServerClient.verify(shardRequest("cursor"), VerificationTimes.exactly(3));
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(3));

        consumer.close();
    }

    @Test
    public void testOffsetReset() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 1), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", -1), HEARTBEAT_RESULT);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockFail(offsetExpectation("commit", 1), 400, OFFSET_RESETED);
        mockSuccess(shardExpectation("cursor", 4), GET_CURSOR_RESULT);
        mockSuccess(shardExpectation("sub", -1), GET_RECORD_RESULT);

        Consumer consumer = getDefaultConsumer();
        Assert.assertNotNull(consumer.read(10));
        sleep(10000);
        Assert.assertNotNull(consumer.read(10)); // commit, reset exception
        sleep(2000);
        try {
            consumer.read(10); // throw reset exception
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.atLeast(1));
        mockServerClient.verify(getTopicRequest(), VerificationTimes.once());
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());
        mockServerClient.verify(offsetRequest("commit"), VerificationTimes.atLeast(0));
        mockServerClient.verify(shardRequest("cursor"), VerificationTimes.exactly(3));
        mockServerClient.verify(shardRequest("sub"), VerificationTimes.atLeast(3));

        consumer.close();
    }

    @Test
    public void testOffsetReset2() {
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 1), JOIN_GROUP_RESULT);
        mockFail(consumerExpectation("heartbeat", -1), 400, OFFSET_RESETED);
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);

        Consumer consumer = getDefaultConsumer();
        try {
            consumer.read(10); // throw reset exception
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.once());
        mockServerClient.verify(getTopicRequest(), VerificationTimes.once());

        consumer.close();
    }
}
