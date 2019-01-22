package com.aliyun.datahub.clientlibrary.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.clientlibrary.producer.Producer;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProducerTest extends BaseTest {
    private final static int SHARD_COUNT = 4;
    private String tupleTopicName;
    private String blobTopicName;
    private DatahubClient pbClient = DatahubClientBuilder.newBuilder().setDatahubConfig(pbDatahubConfig).build();

    @BeforeClass
    public static void setUp() {
        BaseTest.setUp();
        try {
            client.createProject(TEST_PROJECT_NAME, "comment");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            client.deleteProject(TEST_PROJECT_NAME);
        } catch (Exception e) {
            //
        }
    }

    @Before
    public void setUpBeforeTest() {
        tupleTopicName = prepareTupleTopic(schema, SHARD_COUNT);
        blobTopicName = prepareBlobTopic(SHARD_COUNT);
    }

    @After
    public void tearDownAfterTest() {
        cleanProject();
    }

    @Test
    public void testSendTuple() {
        Producer producer = new Producer(TEST_PROJECT_NAME, tupleTopicName, producerConfig);
        for (int i = 0; i < SHARD_COUNT * 5; ++i) {
            producer.send(genTupleRecords(SHARD_COUNT, schema), 1);
        }
        sleep(3000);
        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = "" + i;
            String cursor = client.getCursor(TEST_PROJECT_NAME, tupleTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = client.getRecords(TEST_PROJECT_NAME, tupleTopicName, shardId, schema, cursor, 1000);
            Assert.assertEquals(1000, result.getRecords().size());
        }
    }

    @Test
    public void testSendBlob() {
        Producer producer = new Producer(TEST_PROJECT_NAME, blobTopicName, producerConfig);
        for (int i = 0; i < SHARD_COUNT * 5; ++i) {
            producer.send(genBlobRecords(SHARD_COUNT), 1);
        }
        sleep(3000);
        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = "" + i;
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, blobTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, blobTopicName, shardId, cursor, 1000);
            Assert.assertEquals(1000, result.getRecords().size());
        }
    }

    @Test
    public void testSendWithShardList() {
        Producer producer = new Producer(TEST_PROJECT_NAME, blobTopicName, Arrays.asList("0", "1"), producerConfig);
        for (int i = 0; i < 10; ++i) {
            producer.send(genBlobRecords(SHARD_COUNT), 1);
        }
        sleep(2000);
        for (int i = 0; i < 2; ++i) {
            String shardId = "" + i;
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, blobTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, blobTopicName, shardId, cursor, 1000);
            Assert.assertEquals(1000, result.getRecords().size());
        }
    }

    @Test
    public void testShardOperation() {
        Producer producer = new Producer(TEST_PROJECT_NAME, tupleTopicName, producerConfig);
        for (int i = 0; i < SHARD_COUNT * 2; ++i) {
            producer.send(genTupleRecords(SHARD_COUNT, schema), 1);
        }
        sleep(1000);

        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = "" + i;
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, tupleTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, tupleTopicName, shardId, cursor, 1000);
            Assert.assertTrue(result.getRecords().size() >= 100);
        }
        sleep(5000);

        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");

        for (int i = 0; i < 7; ++i) {
            for (int j = 0; j < SHARD_COUNT + 1; ++j) {
                producer.send(genTupleRecords(SHARD_COUNT, schema), 3);
            }
            sleep(1000);
        }
        for (int i = 0; i < SHARD_COUNT + 1; ++i) {
            String shardId = "" + (i + 1);
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, tupleTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, tupleTopicName, shardId, cursor, 1000);
            Assert.assertTrue(result.getRecords().size() >= 100);
        }

        client.mergeShard(TEST_PROJECT_NAME, tupleTopicName, "1", "2");

        for (int i = 0; i < 7; ++i) {
            for (int j = 0; j < SHARD_COUNT + 1; ++j) {
                producer.send(genTupleRecords(SHARD_COUNT + 1, schema), 3);
            }
            sleep(1000);
        }

        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = "" + (i + 3);
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, tupleTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, tupleTopicName, shardId, cursor, 1000);
            Assert.assertTrue(result.getRecords().size() >= 100);
        }
    }

    @Test
    public void testSendClosed() {
        try {
            Producer producer = new Producer(TEST_PROJECT_NAME, blobTopicName, producerConfig);
            producer.close();
            producer.send(genTupleRecords(SHARD_COUNT, schema), 1);
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("This producer has already been closed", e.getErrorMessage());
        }
    }

    @Test
    public void testInvalidShardIds() {
        try {
            Producer producer = new Producer(TEST_PROJECT_NAME, blobTopicName, Arrays.asList("0", "100"), producerConfig);
            Assert.fail("throw exception");
        } catch (InvalidParameterException e) {
            Assert.assertEquals("Shard must be valid and active", e.getErrorMessage());
        }
    }

    @Test
    public void testMalformedRecords() {
        Producer producer = new Producer(TEST_PROJECT_NAME, tupleTopicName, producerConfig);

        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("a", FieldType.STRING));
            addField(new Field("b", FieldType.BIGINT));
            addField(new Field("c", FieldType.DOUBLE));
            addField(new Field("d", FieldType.TIMESTAMP));
        }};

        try {
            producer.send(Arrays.asList(genTupleData(recordSchema, "0")), 2);
            Assert.fail("throw exception");
        } catch (MalformedRecordException e) {
            Assert.assertEquals("Record field size not match", e.getErrorMessage());
        }
    }

    private class ProducerRunnable implements Runnable {
        private Producer producer;

        ProducerRunnable(Producer producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            for (int i = 0; i < 3; ++i) {
                producer.send(genTupleRecords(SHARD_COUNT, schema), 3);
            }
            sleep(1000);
        }
    }

    @Test
    public void testMultiThreadSend() {
        Producer producer = new Producer(TEST_PROJECT_NAME, tupleTopicName, producerConfig);
        ProducerRunnable runnable = new ProducerRunnable(producer);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < SHARD_COUNT; ++i) {
            threads.add(new Thread(runnable));
            threads.get(i).start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        sleep(2000);

        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = "" + i;
            String cursor = pbClient.getCursor(TEST_PROJECT_NAME, tupleTopicName, shardId, CursorType.OLDEST).getCursor();
            GetRecordsResult result = pbClient.getRecords(TEST_PROJECT_NAME, tupleTopicName, shardId, cursor, 1000);
            Assert.assertTrue(result.getRecords().size() >= 300);
        }
    }
}
