package com.aliyun.datahub.clientlibrary.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.clientlibrary.e2e.common.Configure;
import com.aliyun.datahub.clientlibrary.e2e.common.Constant;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import org.junit.BeforeClass;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseTest {
    protected static DatahubConfig datahubConfig;
    protected static DatahubConfig pbDatahubConfig;
    protected static ConsumerConfig consumerConfig;
    protected static ProducerConfig producerConfig;
    protected static HttpConfig httpConfig;
    protected static DatahubClient client;
    protected static final String TEST_PROJECT_NAME = Configure.getString(Constant.DATAHUB_PROJECT);
    protected RecordSchema schema = new RecordSchema() {{
        addField(new Field("a", FieldType.STRING));
        addField(new Field("b", FieldType.BIGINT));
        addField(new Field("c", FieldType.DOUBLE));
        addField(new Field("d", FieldType.TIMESTAMP));
        addField(new Field("e", FieldType.BOOLEAN));
        addField(new Field("f", FieldType.DECIMAL));
    }};
    private static AtomicLong counter = new AtomicLong();

    @BeforeClass
    public static void setUp() {
        AliyunAccount account = new AliyunAccount(Configure.getString(Constant.DATAHUB_ACCESS_ID),
                Configure.getString(Constant.DATAHUB_ACCESS_KEY));
        httpConfig = new HttpConfig();
        String endpoint = Configure.getString(Constant.DATAHUB_ENDPOINT);
        datahubConfig = new DatahubConfig(endpoint, account);
        pbDatahubConfig = new DatahubConfig(endpoint, account, true);
        consumerConfig = new ConsumerConfig(endpoint, Configure.getString(Constant.DATAHUB_ACCESS_ID), Configure.getString(Constant.DATAHUB_ACCESS_KEY));
        producerConfig = new ProducerConfig(endpoint, Configure.getString(Constant.DATAHUB_ACCESS_ID), Configure.getString(Constant.DATAHUB_ACCESS_KEY));
        client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();
    }

    protected String getTestTopicName(RecordType type) {
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddHHmmss");
        return type.name().toLowerCase() + "_"
                + ft.format(dNow) + "_" + counter.getAndIncrement();
    }

    protected String createSubscription(String topicName) {
        return client.createSubscription(TEST_PROJECT_NAME, topicName, "comment").getSubId();
    }

    protected RecordEntry genTupleData(RecordSchema schema, final String shardId) {
        return genTupleData(schema, shardId, 0);
    }

    protected RecordEntry genTupleData(RecordSchema schema, final String shardId, long id) {
        final TupleRecordData data = new TupleRecordData(schema);
        for (Field field : schema.getFields()) {
            switch (field.getType()) {
                case STRING:
                    data.setField(field.getName(), "string");
                    break;
                case BIGINT:
                    data.setField(field.getName(), id);
                    break;
                case DOUBLE:
                    data.setField(field.getName(), 0.0);
                    break;
                case TIMESTAMP:
                    data.setField(field.getName(), 123456789000000L);
                    break;
                case BOOLEAN:
                    data.setField(field.getName(), true);
                    break;
                case DECIMAL:
                    data.setField(field.getName(), new BigDecimal(10000.000001));
                    break;
                default:
                    throw new DatahubClientException("Unknown field type");
            }
        }

        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setShardId(shardId);
        }};
    }

    protected RecordEntry genBlobData(final String shardId) {
        final BlobRecordData data = new BlobRecordData("test-data".getBytes());
        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setShardId(shardId);
        }};
    }

    protected List<RecordEntry> genTupleRecords(int shardCount, RecordSchema schema) {
        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 300; ++i) {
            result.add(genTupleData(schema, String.valueOf(i % shardCount)));
        }
        return result;
    }

    protected List<RecordEntry> genTupleRecordsInOrder(String shardId, RecordSchema schema, long start) {
        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            result.add(genTupleData(schema, shardId, start));
        }
        return result;
    }

    protected List<RecordEntry> genBlobRecords(int shardCount) {
        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 256; ++i) {
            result.add(genBlobData(String.valueOf(i % shardCount)));
        }
        return result;
    }

    protected void produceTupleRecords(String topicName, RecordSchema schema, int shardCount) {
        for (int i = 0; i < 10; ++i) {
            List<RecordEntry> records = new ArrayList<RecordEntry>(genTupleRecords(shardCount, schema));
            client.putRecords(TEST_PROJECT_NAME, topicName, records);
        }
    }

    protected void produceTupleRecordsInOrder(final String topicName, final RecordSchema schema, int shardCount) {
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j < shardCount; ++j) {
            final String shardId = "" + j;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; ++i) {
                        List<RecordEntry> records = new ArrayList<RecordEntry>(genTupleRecordsInOrder(shardId, schema, 0));
                        client.putRecords(TEST_PROJECT_NAME, topicName, records);
                    }
                }
            });
            t.start();
            threads.add(t);
        }
        for (int j = 0; j < shardCount; ++j) {
            try {
                threads.get(j).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected void produceBlobRecords(String topicName, int shardCount) {
        for (int i = 0; i < 10; ++i) {
            List<RecordEntry> records = new ArrayList<RecordEntry>(genBlobRecords(shardCount));
            client.putRecords(TEST_PROJECT_NAME, topicName, records);
        }
    }

    protected String prepareTupleTopic(RecordSchema schema, int shardCount) {
        String tupleTopicName = getTestTopicName(RecordType.TUPLE);
        client.createTopic(TEST_PROJECT_NAME, tupleTopicName, shardCount, 1, RecordType.TUPLE, schema, "test tuple topic");
        client.waitForShardReady(TEST_PROJECT_NAME, tupleTopicName);
        return tupleTopicName;
    }

    protected String prepareBlobTopic(int shardCount) {
        String blobTopicName = getTestTopicName(RecordType.BLOB);
        client.createTopic(TEST_PROJECT_NAME, blobTopicName, shardCount, 1, RecordType.BLOB, "test blob topic");
        client.waitForShardReady(TEST_PROJECT_NAME, blobTopicName);
        return blobTopicName;
    }

    protected void cleanProject() {
        try {
            ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT_NAME);
            for (String topic : listTopicResult.getTopicNames()) {
                ListSubscriptionResult listSubscriptionResult = client.listSubscription(TEST_PROJECT_NAME, topic, 1, 100);
                for (SubscriptionEntry subscriptionEntry : listSubscriptionResult.getSubscriptions()) {
                    client.deleteSubscription(TEST_PROJECT_NAME, topic, subscriptionEntry.getSubId());
                }
                client.deleteTopic(TEST_PROJECT_NAME, topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
