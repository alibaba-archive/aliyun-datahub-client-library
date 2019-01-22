package com.aliyun.datahub.clientlibrary.e2e;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.consumer.ShardGroupReader;
import com.aliyun.datahub.clientlibrary.consumer.ShardReader;
import com.aliyun.datahub.clientlibrary.models.Offset;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardReaderGroupTest extends BaseTest {
    private final static int SHARD_COUNT = 4;
    private String tupleTopicName;
    private String blobTopicName;

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

    ShardGroupReader getDefaultFetcherGroup(String topic) {
        return new ShardGroupReader(TEST_PROJECT_NAME, topic, consumerConfig);
    }

    private Map<String, ShardReader> getShardReaderMap(ShardGroupReader shardGroupReader) {
        try {
            java.lang.reflect.Field field = ShardGroupReader.class.getDeclaredField("shardReaderMap");
            field.setAccessible(true);
            return (Map<String, ShardReader>) field.get(shardGroupReader);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void testReadTuple() {
        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);
        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));

        shardGroupReader.createShardReader(offsetMap);

        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            RecordEntry RecordEntry = shardGroupReader.read();
            if (RecordEntry != null) {
                result.add(RecordEntry);
            }
            if (result.size() > 0) {
                break;
            }
            sleep(1000);
        }
        Assert.assertFalse(result.isEmpty());
        Map<String, Long> sequenceMap = new HashMap<>();
        for (RecordEntry RecordEntry : result) {
            long sequence = sequenceMap.containsKey(RecordEntry.getShardId()) ? sequenceMap.get(RecordEntry.getShardId()) : 0;
            Assert.assertEquals(sequence + 1, RecordEntry.getSequence());
            sequenceMap.put(RecordEntry.getShardId(), sequence + 1);
        }
    }

    @Test
    public void testReadBlob() {
        produceBlobRecords(blobTopicName, SHARD_COUNT);
        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(blobTopicName);

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset().setSequence(1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));

        shardGroupReader.createShardReader(offsetMap);
        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            RecordEntry RecordEntry = shardGroupReader.read();
            if (RecordEntry != null) {
                result.add(RecordEntry);
            }
            if (result.size() > 0) {
                break;
            }
            sleep(1000);
        }
        Assert.assertFalse(result.isEmpty());
        Map<String, Long> sequenceMap = new HashMap<>();
        for (RecordEntry RecordEntry : result) {
            long sequence = sequenceMap.containsKey(RecordEntry.getShardId()) ? sequenceMap.get(RecordEntry.getShardId()) : 0;
            Assert.assertEquals(sequence + 1, RecordEntry.getSequence());
            sequenceMap.put(RecordEntry.getShardId(), sequence + 1);
        }
    }

    @Test
    public void testCreateNewFetcher() {
        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);

        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));

        shardGroupReader.createShardReader(offsetMap);
        Assert.assertEquals(3, getShardReaderMap(shardGroupReader).size());

        shardGroupReader.removeShardReader(new ArrayList<>(offsetMap.keySet()));
        Assert.assertTrue(getShardReaderMap(shardGroupReader).isEmpty());
    }

    @Test
    public void testTopicNotFound() {
        try {
            ShardGroupReader shardGroupReader = new ShardGroupReader(TEST_PROJECT_NAME, "NOT_FOUND", consumerConfig);
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("The specified topic name does not exist.", e.getErrorMessage());
        }
    }

    @Test
    public void testShardNotExisted() {
        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("" + SHARD_COUNT, new Offset(1, 1));
        try {
            shardGroupReader.createShardReader(offsetMap);
            for (int i = 0; i < 10; ++i) {
                shardGroupReader.read();
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertTrue(e.getErrorMessage().contains("ShardId Not Exist. Invalid shard id:"));
        }
    }

    @Test
    public void testEmptyOffset() {
        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset());
        try {
            shardGroupReader.createShardReader(offsetMap);
            for (int i = 0; i < 10; ++i) {
                shardGroupReader.read();
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (InvalidParameterException e) {
            Assert.assertEquals("Sequence and system time are all invalid", e.getErrorMessage());
        }
    }

    @Test
    public void testInvalidSequenceWithTimestamp() {
        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(Long.MAX_VALUE, 0));
        shardGroupReader.createShardReader(offsetMap);

        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 21; ++i) {
            RecordEntry RecordEntry = shardGroupReader.read();
            if (RecordEntry != null) {
                result.add(RecordEntry);
            }
            if (result.size() > 0) {
                break;
            }
            sleep(1000);
        }
        Assert.assertFalse(result.isEmpty());
        Map<String, Long> sequenceMap = new HashMap<>();
        for (RecordEntry RecordEntry : result) {
            long sequence = sequenceMap.containsKey(RecordEntry.getShardId()) ? sequenceMap.get(RecordEntry.getShardId()) + 1 : 0;
            Assert.assertEquals(sequence, RecordEntry.getSequence());
            sequenceMap.put(RecordEntry.getShardId(), sequence);
        }
    }

    @Test
    public void testInvalidSequenceWithoutTimestamp() {
        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset().setSequence(Long.MAX_VALUE));
        shardGroupReader.createShardReader(offsetMap);

        try {
            for (int i = 0; i < 12; ++i) {
                shardGroupReader.read();
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (InvalidParameterException e) {
            Assert.assertEquals("Time in seek request is out of range.", e.getErrorMessage());
        }
    }

    @Test
    public void testInvalidSequenceWithInvalidTimestamp() {
        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        ShardGroupReader shardGroupReader = getDefaultFetcherGroup(tupleTopicName);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(Long.MAX_VALUE, Long.MAX_VALUE));
        shardGroupReader.createShardReader(offsetMap);

        try {
            for (int i = 0; i < 11; ++i) {
                shardGroupReader.read();
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (InvalidParameterException e) {
            Assert.assertEquals("Time in seek request is out of range.", e.getErrorMessage());
        }
    }
}
