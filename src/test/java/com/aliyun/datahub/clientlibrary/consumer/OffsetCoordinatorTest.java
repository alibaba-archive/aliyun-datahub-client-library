package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.clientlibrary.MockServer;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetCoordinatorTest extends MockServer {
    OffsetCoordinator getDefaultOffsetCoordinator() {
        ConsumerConfig config = new ConsumerConfig(serverEndpoint, "test_ak", "test_sk", "test_token");
        config.setOffsetCommitTimeoutMs(10000);
        return new OffsetCoordinator("test_project", "test_topic", "test_sub_id", config);
    }

    @Test
    public void testCommit() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockSuccess(offsetExpectation("commit", -1), "");

        OffsetCoordinator coordinator = getDefaultOffsetCoordinator();
        sleep(10000);

        coordinator.commitIfNeeded(); // empty offset, not commit

        coordinator.openAndGetOffsets(Arrays.asList("0", "1", "2"));
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());

        coordinator.commitIfNeeded(); // commit
        coordinator.commitIfNeeded(); // wait for interval, not commit
        mockServerClient.verify(offsetRequest("commit"), VerificationTimes.once());
        coordinator.close();
    }

    @Test
    public void testOffsetReset() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockFail(offsetExpectation("commit", 1), 400, OFFSET_RESETED);
        mockSuccess(offsetExpectation("get", 1), GET_OFFSET_RESULT);

        OffsetCoordinator coordinator = getDefaultOffsetCoordinator();
        sleep(10000);

        coordinator.commitIfNeeded(); // not commit

        coordinator.openAndGetOffsets(Arrays.asList("0", "1", "2"));
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());

        coordinator.commitIfNeeded(); // commit async, reset exception
        sleep(2000);
        try {
            coordinator.commitIfNeeded(); // throw exception
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        try {
            coordinator.commitIfNeeded(); // throw exception
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        mockServerClient.verify(offsetRequest("commit"), VerificationTimes.once());
        coordinator.close();
    }

    @Test
    public void testCommitRetryLimitExceeded() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockFail(offsetExpectation("commit", -1), 500, INTERNAL_SERVER_ERROR);

        OffsetCoordinator coordinator = getDefaultOffsetCoordinator();
        sleep(10000);

        coordinator.openAndGetOffsets(Arrays.asList("0", "1", "2"));
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());

        try {
            coordinator.commitIfNeeded();
            sleep(5000);

            coordinator.commitIfNeeded();
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("Service internal error, please try again later.", e.getErrorMessage());
        }

        mockServerClient.verify(offsetRequest("commit"), VerificationTimes.atLeast(1));
        coordinator.close();
    }

    @Test
    public void testGetReadEndShardList() {
        mockSuccess(getTopicExpectation(1), GET_TOPIC_RESULT);
        mockSuccess(listShardExpectation(2), LIST_SHARD_RESULT);
        mockSuccess(offsetExpectation("open", 1), INIT_OFFSET_RESULT);
        mockSuccess(offsetExpectation("commit", -1), "");

        OffsetCoordinator coordinator = getDefaultOffsetCoordinator();
        coordinator.openAndGetOffsets(Arrays.asList("0", "1", "2"));
        mockServerClient.verify(offsetRequest("open"), VerificationTimes.once());

        coordinator.setOffset("0", 1, 1);
        sleep(10000);
        coordinator.commitIfNeeded();
        sleep(1000);

        Map<String, Long> endSequenceMap = new HashMap<>();
        endSequenceMap.put("0", 1L);
        List<String> readEndShardList = coordinator.getReadEndShardList(endSequenceMap);
        Assert.assertEquals(1, readEndShardList.size());
        Assert.assertEquals("0", readEndShardList.get(0));
        coordinator.close();
    }
}
