package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.clientlibrary.MockServer;
import com.aliyun.datahub.clientlibrary.common.Constants;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ShardCoordinatorTest extends MockServer {
    ShardCoordinator getDefaultShardCoordinator() {
        return new ShardCoordinator("test_project", "test_topic", "test_sub_id",
                new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token"));
    }

    private void waitFirstHeartbeat(ShardCoordinator coordinator) {
        for (int i = 0; i < 60; ++i) {
            if (getPlanVersion(coordinator) != Constants.DEFAULT_PLAN_VERSION) {
                break;
            }
            sleep(1000);
        }
    }

    private Set<String> getNewAssignment(ShardCoordinator shardCoordinator) {
        try {
            java.lang.reflect.Field field = ShardCoordinator.class.getDeclaredField("heartbeat");
            field.setAccessible(true);
            Heartbeat heartbeat = (Heartbeat) field.get(shardCoordinator);

            Method method = Heartbeat.class.getDeclaredMethod("getShards");
            method.setAccessible(true);
            return (Set<String>) method.invoke(heartbeat);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    private long getPlanVersion(ShardCoordinator shardCoordinator) {
        try {
            java.lang.reflect.Field field = ShardCoordinator.class.getDeclaredField("heartbeat");
            field.setAccessible(true);
            Heartbeat heartbeat = (Heartbeat) field.get(shardCoordinator);

            Method method = Heartbeat.class.getDeclaredMethod("getPlanVersion");
            method.setAccessible(true);
            return (long) method.invoke(heartbeat);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return Constants.DEFAULT_PLAN_VERSION;
    }

    @Test
    public void testJoinGroup() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 1), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", 1), HEARTBEAT_RESULT);

        ShardCoordinator coordinator = getDefaultShardCoordinator();

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.atLeast(1));
        coordinator.close();
    }

    @Test
    public void testJoinGroupWithInvalidSubId() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockFail(consumerExpectation("joinGroup", 4), 404, NO_SUCH_SUBSCRIPTION);

        try {
            ShardCoordinator coordinator = getDefaultShardCoordinator();
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("Subscription id invalid:", e.getErrorMessage());
        }
        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.exactly(4));
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.exactly(0));
    }

    @Test
    public void testSyncGroupNeedRejoin() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 2), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", 2), HEARTBEAT_RESULT);
        mockFail(consumerExpectation("syncGroup", 1), 400, NO_SUCH_CONSUMER);

        ShardCoordinator coordinator = getDefaultShardCoordinator();

        waitFirstHeartbeat(coordinator);
        Assert.assertEquals(3, getNewAssignment(coordinator).size());

        List<String> releaseShardList = new ArrayList<>();
        releaseShardList.add("1");

        List<String> readEndShardList = new ArrayList<>();
        readEndShardList.add("2");

        coordinator.syncGroup(releaseShardList, readEndShardList);

        coordinator.rejoinIfNeeded();

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.exactly(2));
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.exactly(2));
        mockServerClient.verify(consumerRequest("syncGroup"), VerificationTimes.once());
        coordinator.close();
    }

    @Test
    public void testRejoinIfNeeded() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 2), JOIN_GROUP_RESULT);
        mockFail(consumerExpectation("heartbeat", -1), 400, NO_SUCH_CONSUMER);

        ShardCoordinator coordinator = getDefaultShardCoordinator();
        waitFirstHeartbeat(coordinator);

        coordinator.rejoinIfNeeded();

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.exactly(2));
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.atLeast(1));
        coordinator.close();
    }

    @Test
    public void testSyncGroupWithResetException() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 2), JOIN_GROUP_RESULT);
        mockSuccess(consumerExpectation("heartbeat", -1), HEARTBEAT_RESULT);
        mockFail(consumerExpectation("syncGroup", 1), 400, OFFSET_RESETED);

        ShardCoordinator coordinator = getDefaultShardCoordinator();
        waitFirstHeartbeat(coordinator);

        try {
            coordinator.syncGroup(Arrays.asList("0", "1"), Arrays.asList("2", "3"));
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("syncGroup"), VerificationTimes.once());
        coordinator.close();
    }

    @Test
    public void testRejoinWithResetException() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(consumerExpectation("joinGroup", 2), JOIN_GROUP_RESULT);
        mockFail(consumerExpectation("heartbeat", -1), 400, OFFSET_RESETED);

        ShardCoordinator coordinator = getDefaultShardCoordinator();
        waitFirstHeartbeat(coordinator);

        try {
            coordinator.rejoinIfNeeded();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        try {
            coordinator.rejoinIfNeeded();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        mockServerClient.verify(consumerRequest("joinGroup"), VerificationTimes.once());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.once());
        coordinator.close();
    }
}
