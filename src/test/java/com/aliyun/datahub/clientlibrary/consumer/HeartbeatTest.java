package com.aliyun.datahub.clientlibrary.consumer;

import com.aliyun.datahub.client.exception.SubscriptionOffsetResetException;
import com.aliyun.datahub.clientlibrary.MockServer;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.verify.VerificationTimes;

public class HeartbeatTest extends MockServer {
    Heartbeat getDefaultHeartbeat() {
        return new Heartbeat(client, "test_project", "test_topic", "test_sub_id");
    }

    @Test
    public void testStart() {
        Heartbeat heartbeat = getDefaultHeartbeat();
        mockSuccess(consumerExpectation("heartbeat", -1), HEARTBEAT_RESULT);

        heartbeat.start("test_consumer", 1, 30000);
        Assert.assertTrue(heartbeat.checkRunning());

        for (int i = 0; i < 10; ++i) {
            if (!heartbeat.getShards().isEmpty()) {
                break;
            }
            sleep(1000);
        }

        Assert.assertEquals(3, heartbeat.getShards().size());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.once());
        heartbeat.stop();
    }

    @Test
    public void testStop() {
        Heartbeat heartbeat = getDefaultHeartbeat();
        mockFail(consumerExpectation("heartbeat", -1), 400, NO_SUCH_CONSUMER);

        heartbeat.start("test_consumer", 1, 30000);

        for (int i = 0; i < 10; ++i) {
            if (!heartbeat.checkRunning()) {
                break;
            }
            sleep(1000);
        }
        Assert.assertTrue(!heartbeat.checkRunning());
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.exactly(4));
        heartbeat.stop();
    }

    @Test
    public void testReset() {
        Heartbeat heartbeat = getDefaultHeartbeat();
        mockFail(consumerExpectation("heartbeat", -1), 400, OFFSET_RESETED);

        heartbeat.start("test_consumer", 1, 30000);

        try {
            for (int i = 0; i < 10; ++i) {
                if (!heartbeat.checkRunning()) {
                    break;
                }
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }

        try {
            heartbeat.checkRunning();
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }
        mockServerClient.verify(consumerRequest("heartbeat"), VerificationTimes.once());
        heartbeat.stop();
    }
}
