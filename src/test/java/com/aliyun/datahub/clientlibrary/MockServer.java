package com.aliyun.datahub.clientlibrary;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockserver.client.server.ForwardChainExpectation;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.StringBody;

import java.nio.charset.StandardCharsets;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

public class MockServer {
    protected final static String LOCALHOST = "127.0.0.1";

    protected static ClientAndServer mockServer;
    protected static MockServerClient mockServerClient;
    protected static DatahubConfig datahubConfig;
    protected static DatahubClient client;
    protected static int listenPort;
    protected static String serverEndpoint;

    private Header header1 = new Header("x-datahub-request-id", "testId");
    private Header header2 = new Header("Content-Type", "application/json");

    protected static String SUBSCRIPTION_PATH = "/projects/test_project/topics/test_topic/subscriptions/test_sub_id";
    protected static String TOPIC_PATH = "/projects/test_project/topics/test_topic";
    protected static String OFFSETS_PATH = "/projects/test_project/topics/test_topic/subscriptions/test_sub_id/offsets";
    protected static String SHARD_REGEX_PATH = "/projects/test_project/topics/test_topic/shards/.*";
    protected static String SHARDS_PATH = "/projects/test_project/topics/test_topic/shards";

    protected static String JOIN_GROUP_RESULT = "{\"ConsumerId\":\"test\",\"VersionId\":1,\"SessionTimeout\":60000}";
    protected static String HEARTBEAT_RESULT = "{\"ShardList\":[\"0\",\"1\",\"2\"],\"TotalPlan\":\"test\",\"PlanVersion\":1}";
    protected static String GET_TOPIC_RESULT = "{\"Comment\": \"test topic tuple\",\"CreateTime\": 1525763481,\"LastModifyTime\": 1525763481,\"Lifecycle\": 1,\"RecordSchema\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"f1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"f2\\\",\\\"type\\\":\\\"BIGINT\\\"}]}\",\"RecordType\": \"TUPLE\",\"ShardCount\": 4}";
    protected static String INIT_OFFSET_RESULT = "{\"Offsets\":{\"0\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1},\"1\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1},\"2\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1}}}";
    protected static String GET_OFFSET_RESULT = "{\"Offsets\":{\"0\":{\"Sequence\":0,\"SessionId\":1,\"Timestamp\":0,\"Version\":2},\"1\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1},\"2\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1}}}";
    protected static String GET_CURSOR_RESULT = "{\"Cursor\":\"200000000001000000000000cfac5050\",\"RecordTime\":1532411365149,\"Sequence\":1285}";
    protected static String GET_RECORD_RESULT = "{\"NextCursor\":\"200000000001000000000000d3bc50f0\",\"RecordCount\":10,\"StartSeq\":2,\"Records\":[{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d0145060\",\"Cursor\":\"200000000001000000000000cfac5050\",\"Sequence\":2,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d07c5070\",\"Cursor\":\"200000000001000000000000d0145060\",\"Sequence\":3,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d0e45080\",\"Cursor\":\"200000000001000000000000d07c5070\",\"Sequence\":4,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d14c5090\",\"Cursor\":\"200000000001000000000000d0e45080\",\"Sequence\":5,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d1b450a0\",\"Cursor\":\"200000000001000000000000d14c5090\",\"Sequence\":6,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d21c50b0\",\"Cursor\":\"200000000001000000000000d1b450a0\",\"Sequence\":7,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d28450c0\",\"Cursor\":\"200000000001000000000000d21c50b0\",\"Sequence\":8,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d2ec50d0\",\"Cursor\":\"200000000001000000000000d28450c0\",\"Sequence\":9,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d35450e0\",\"Cursor\":\"200000000001000000000000d2ec50d0\",\"Sequence\":10,\"Data\":[\"1465999335123456\",\"30\"]},{\"SystemTime\":1532411365149,\"NextCursor\":\"200000000001000000000000d3bc50f0\",\"Cursor\":\"200000000001000000000000d35450e0\",\"Sequence\":11,\"Data\":[\"1465999335123456\",\"30\"]}]}";
    protected static String LIST_SHARD_RESULT = "{\"Shards\":[{\"BeginHashKey\":\"00000000000000000000000000000000\",\"BeginKey\":\"00000000000000000000000000000000\",\"Cluster\":\"AT-ODPS-TEST\",\"CreateTime\":1534575043,\"EndHashKey\": \"3FFFFFFFFFFFFFFF3FFFFFFFFFFFFFFF\",\"EndKey\": \"3FFFFFFFFFFFFFFF3FFFFFFFFFFFFFFF\",\"LeftShardId\": \"4294967295\",\"ParentShardIds\": [],\"RightShardId\": \"1\",\"ShardId\": \"0\",\"State\": \"ACTIVE\",\"Worker\": \"Datahub/XStreamServicexishao4/XStreamBroker@rs3b12026.et2sqa\"},{\"BeginHashKey\": \"3FFFFFFFFFFFFFFF3FFFFFFFFFFFFFFF\",\"BeginKey\": \"3FFFFFFFFFFFFFFF3FFFFFFFFFFFFFFF\",\"Cluster\": \"AT-ODPS-TEST\",\"CreateTime\": 1534575043,\"EndHashKey\": \"7FFFFFFFFFFFFFFE7FFFFFFFFFFFFFFE\",\"EndKey\": \"7FFFFFFFFFFFFFFE7FFFFFFFFFFFFFFE\",\"LeftShardId\": \"0\",\"ParentShardIds\": [],\"RightShardId\": \"2\",\"ShardId\": \"1\",\"State\": \"ACTIVE\",\"Worker\": \"Datahub/XStreamServicexishao4/XStreamBroker@rs3b12026.et2sqa\"},{\"BeginHashKey\": \"7FFFFFFFFFFFFFFE7FFFFFFFFFFFFFFE\",\"BeginKey\": \"7FFFFFFFFFFFFFFE7FFFFFFFFFFFFFFE\",\"Cluster\": \"AT-ODPS-TEST\",\"CreateTime\": 1534575043,\"EndHashKey\": \"BFFFFFFFFFFFFFFDBFFFFFFFFFFFFFFD\",\"EndKey\": \"BFFFFFFFFFFFFFFDBFFFFFFFFFFFFFFD\",\"LeftShardId\": \"1\",\"ParentShardIds\": [],\"RightShardId\": \"3\",\"ShardId\": \"2\",\"State\": \"ACTIVE\",\"Worker\": \"Datahub/XStreamServicexishao4/XStreamBroker@rs3b12026.et2sqa\"},{\"BeginHashKey\": \"BFFFFFFFFFFFFFFDBFFFFFFFFFFFFFFD\",\"BeginKey\": \"BFFFFFFFFFFFFFFDBFFFFFFFFFFFFFFD\",\"Cluster\": \"AT-ODPS-TEST\",\"CreateTime\": 1534575043,\"EndHashKey\": \"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"EndKey\": \"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"LeftShardId\": \"2\",\"ParentShardIds\": [],\"RightShardId\": \"4294967295\",\"ShardId\": \"3\",\"State\": \"ACTIVE\",\"Worker\": \"Datahub/XStreamServicexishao4/XStreamBroker@rs3b12026.et2sqa\"}]}";

    protected static String NO_SUCH_SUBSCRIPTION = "{\"ErrorCode\":\"NoSuchSubscription\",\"ErrorMessage\":\"Subscription id invalid:\"}";
    protected static String SHARD_SEALED = "{\"ErrorCode\":\"InvalidShardOperation\",\"ErrorMessage\":\"The specified shard is not active.\"}";
    protected static String NO_SUCH_SHARD = "{\"ErrorCode\":\"NoSuchShard\",\"ErrorMessage\":\"ShardId Not Exist. Invalid shard id\"}";
    protected static String NO_SUCH_TOPIC = "{\"ErrorCode\":\"NoSuchTopic\",\"ErrorMessage\":\"The specified topic name does not exist.\"}";
    protected static String NO_SUCH_CONSUMER = "{\"ErrorCode\":\"NoSuchConsumer\",\"ErrorMessage\":\"The specified consumer does not exist.\"}";
    protected static String GROUP_IN_PROCESS = "{\"ErrorCode\":\"ConsumerGroupInProcess\",\"ErrorMessage\":\"The consumer group is in process.\"}";
    protected static String INTERNAL_SERVER_ERROR = "{\"ErrorCode\":\"InternalServerError\",\"ErrorMessage\":\"Service internal error, please try again later.\"}";
    protected static String INVALID_SEEK_PARAM = "{\"ErrorCode\":\"InvalidParameter\",\"ErrorMessage\":\"Time in seek request is out of range.\"}";
    protected static String INVALID_CURSOR = "{\"ErrorCode\":\"InvalidCursor\",\"ErrorMessage\":\"The cursor is expired.\"}";
    protected static String OFFSET_RESETED = "{\"ErrorCode\": \"OffsetReseted\",\"ErrorMessage\": \"Offset has reseted\"}";

    @BeforeClass
    public static void setUp() {
        mockServer = ClientAndServer.startClientAndServer(0);
        listenPort = mockServer.getPort();
        serverEndpoint = LOCALHOST + ":" + listenPort;

        mockServerClient = new MockServerClient(LOCALHOST, listenPort);

        datahubConfig = new DatahubConfig(
                serverEndpoint,
                new AliyunAccount("test_ak", "test_sk", "test_token"),
                false
        );
        client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();
    }

    protected static RecordSchema genRecordSchema() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        schema.addField(new Field("f2", FieldType.BIGINT));
        return schema;
    }


    @Before
    public void clear() {
        mockServer.reset();
    }

    @AfterClass
    public static void tearDown() {
        mockServer.stop();
    }

    protected static StringBody exact(String body) {
        return StringBody.exact(body, StandardCharsets.UTF_8);
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected ForwardChainExpectation consumerExpectation(String action, int times) {
        if (times <= 0) {
            return mockServerClient.when(request()
                    .withPath(SUBSCRIPTION_PATH)
                    .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)));
        }
        return mockServerClient.when(request()
                        .withPath(SUBSCRIPTION_PATH)
                        .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)),
                Times.exactly(times));
    }

    protected ForwardChainExpectation getTopicExpectation(int times) {
        if (times <= 0) {
            return mockServerClient.when(request().withPath(TOPIC_PATH));
        }
        return mockServerClient.when(request().withPath(TOPIC_PATH), Times.exactly(times));
    }

    protected ForwardChainExpectation listShardExpectation(int times) {
        if (times <= 0) {
            return mockServerClient.when(request().withPath(SHARDS_PATH));
        }
        return mockServerClient.when(request().withPath(SHARDS_PATH), Times.exactly(times));
    }

    protected ForwardChainExpectation offsetExpectation(String action, int times) {
        if (times <= 0) {
            return mockServerClient.when(request().withPath(OFFSETS_PATH)
                    .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)));
        }
        return mockServerClient.when(request().withPath(OFFSETS_PATH)
                        .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)),
                Times.exactly(times));
    }

    protected ForwardChainExpectation shardExpectation(String action, int times) {
        if (times <= 0) {
            return mockServerClient.when(request().withPath(SHARD_REGEX_PATH)
                    .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)));
        }
        return mockServerClient.when(request().withPath(SHARD_REGEX_PATH)
                        .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS)),
                Times.exactly(times));
    }

    protected ForwardChainExpectation cursorExpectation(String type, int times) {
        if (times <= 0) {
            return mockServerClient.when(request().withPath(SHARD_REGEX_PATH)
                    .withBody(json("{\"Action\":\"cursor\",\"Type\": \"" + type + "\"}", MatchType.ONLY_MATCHING_FIELDS)));
        }
        return mockServerClient.when(request().withPath(SHARD_REGEX_PATH)
                        .withBody(json("{\"Action\":\"cursor\",\"Type\": \"" + type + "\"}", MatchType.ONLY_MATCHING_FIELDS)),
                Times.exactly(times));
    }

    protected void mockSuccess(ForwardChainExpectation expectation, String body) {
        expectation.respond(response().withStatusCode(200).withHeaders(header1, header2).withBody(body));
    }

    protected void mockFail(ForwardChainExpectation expectation, int code, String body) {
        expectation.respond(response().withStatusCode(code).withHeaders(header1, header2).withBody(body));
    }

    protected HttpRequest consumerRequest(String action) {
        return request().withMethod("POST").withPath(SUBSCRIPTION_PATH)
                .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS));
    }

    protected HttpRequest offsetRequest(String action) {
        return request().withPath(OFFSETS_PATH)
                .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS));
    }

    protected HttpRequest shardRequest(String action) {
        return request().withMethod("POST").withPath(SHARD_REGEX_PATH)
                .withBody(json("{\"Action\":\"" + action + "\"}", MatchType.ONLY_MATCHING_FIELDS));
    }

    protected HttpRequest cursorRequest(String type) {
        return request().withMethod("POST").withPath(SHARD_REGEX_PATH)
                .withBody(json("{\"Action\":\"cursor\",\"Type\":\"" + type + "\"}", MatchType.ONLY_MATCHING_FIELDS));
    }

    protected HttpRequest getTopicRequest() {
        return request().withMethod("GET").withPath(TOPIC_PATH);
    }

    protected HttpRequest listShardRequest() {
        return request().withMethod("GET").withPath(SHARDS_PATH);
    }
}