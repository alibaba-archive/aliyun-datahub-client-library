package com.aliyun.datahub.clientlibrary.e2e;

import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.OpenSubscriptionSessionResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.clientlibrary.consumer.*;
import org.junit.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

public class ConsumerTest extends BaseTest {
    private final static int SHARD_COUNT = 100;
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

    private Set<String> getNewAssignment(Consumer consumer) {
        try {
            java.lang.reflect.Field field = Consumer.class.getDeclaredField("shardCoordinator");
            field.setAccessible(true);
            ShardCoordinator shardCoordinator = (ShardCoordinator) field.get(consumer);

            field = ShardCoordinator.class.getDeclaredField("heartbeat");
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

    private Set<String> getAssignment(Consumer consumer) {
        try {
            java.lang.reflect.Field field = Consumer.class.getDeclaredField("shardGroupReader");
            field.setAccessible(true);
            ShardGroupReader shardGroupReader = (ShardGroupReader) field.get(consumer);

            field = ShardGroupReader.class.getDeclaredField("shardReaderMap");
            field.setAccessible(true);
            Map<String, ShardReader> shardReaderMap = (Map<String, ShardReader>) field.get(shardGroupReader);
            return shardReaderMap.keySet();
        } catch ( IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void waitShardRelease(Consumer consumer) {
        waitShardRelease(Collections.singletonList(consumer));
    }

    private void waitShardRelease(List<Consumer> consumers) {
        for (int i = 0; i < 300; ++i) {
            int total = 0;
            for (Consumer consumer : consumers) {
                total += getNewAssignment(consumer).size() < getAssignment(consumer).size() ? 1 : 0;
            }
            if (total == consumers.size()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void waitShardAllAssigned(Consumer consumer, int shardCount) {
        waitShardAllAssigned(Collections.singletonList(consumer), shardCount);
    }

    private void waitShardAllAssigned(List<Consumer> consumers, int shardCount) {
        for (int i = 0; i < 400; ++i) {
            if (isAllAssigned(consumers, shardCount)) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isAllAssigned(Consumer consumer, int shardCount) {
        return isAllAssigned(Collections.singletonList(consumer), shardCount);
    }

    private boolean isAllAssigned(List<Consumer> consumers, int shardCount) {
        int total = 0;
        int minSize = shardCount;
        int maxSize = 0;
        Set<String> merge = new HashSet<>();
        for (Consumer consumer : consumers) {
            Set<String> assignment = getNewAssignment(consumer);
            if (assignment.isEmpty()) {
                return false;
            }
            minSize = Math.min(minSize, assignment.size());
            maxSize = Math.max(maxSize, assignment.size());
            total += assignment.size();
            merge.addAll(assignment);
        }
        return merge.size() == total &&
                minSize == shardCount / consumers.size() &&
                maxSize == (shardCount + consumers.size() - 1) / consumers.size();
    }

    private boolean checkRecordOrder(Map<String, List<RecordEntry>> recordMap, RecordEntry recordEntry) {
        if (recordEntry == null) {
            return false;
        }
        if (!recordMap.containsKey(recordEntry.getShardId())) {
            recordMap.put(recordEntry.getShardId(), new ArrayList<RecordEntry>());
        }
        List<RecordEntry> list = recordMap.get(recordEntry.getShardId());
        if (!list.isEmpty()) {
            if (recordEntry.getSequence() > list.get(list.size() - 1).getSequence() + 1) {
                System.out.println("shardId: " + recordEntry.getShardId() + ", lastSequence: " +
                        list.get(list.size() - 1).getSequence() + ", sequence: " + recordEntry.getSequence());
                return false;
            }
        }
        list.add(recordEntry);
        return true;
    }

    @Test
    public void testAutoAssignConsumeTuple() {
        String subId = createSubscription(tupleTopicName);

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        RecordEntry recordEntry = consumer.read(300);

        Assert.assertNotNull(recordEntry);
        Assert.assertEquals(0, recordEntry.getSequence());

        consumer.close();
    }

    @Test
    public void testAutoAssignConsumeBlob() {
        String subId = createSubscription(blobTopicName);

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, blobTopicName, subId, consumerConfig);

        produceBlobRecords(blobTopicName, SHARD_COUNT);

        RecordEntry recordEntry = consumer.read(100);

        Assert.assertNotNull(recordEntry);
        Assert.assertEquals(0, recordEntry.getSequence());

        consumer.close();
    }

    @Test
    public void testMultiConsumer() {
        String subId = createSubscription(tupleTopicName);

        List<Consumer> consumers = new ArrayList<>();

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        RecordEntry recordEntry = consumers.get(0).read(500);
        Assert.assertNotNull(recordEntry);
        Assert.assertTrue(isAllAssigned(consumers.subList(0, 1), SHARD_COUNT));

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        waitShardRelease(consumers.get(0));

        consumers.get(0).read(500);
        recordEntry = consumers.get(1).read(500);

        Assert.assertNotNull(recordEntry);
        Assert.assertTrue(isAllAssigned(consumers.subList(0, 2), SHARD_COUNT));

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        waitShardRelease(consumers.subList(0, 2));

        consumers.get(0).read(500);
        consumers.get(1).read(500);
        recordEntry = consumers.get(2).read(500);

        Assert.assertNotNull(recordEntry);
        Assert.assertTrue(isAllAssigned(consumers.subList(0, 3), SHARD_COUNT));

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        waitShardRelease(consumers.subList(0, 3));

        consumers.get(0).read(500);
        consumers.get(1).read(500);
        consumers.get(2).read(500);
        recordEntry = consumers.get(3).read(500);

        Assert.assertNotNull(recordEntry);
        Assert.assertTrue(isAllAssigned(consumers.subList(0, 4), SHARD_COUNT));

        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testMultiJoinGroup() {
        String subId = createSubscription(tupleTopicName);

        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < SHARD_COUNT; ++i) {
            consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        }

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        waitShardAllAssigned(consumers, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumers, SHARD_COUNT));

        for (Consumer consumer : consumers) {
            RecordEntry recordEntry = consumer.read(10);
            Assert.assertNotNull(recordEntry);
        }

        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testIdleConsumer() {
        String subId = createSubscription(tupleTopicName);

        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < SHARD_COUNT; ++i) {
            consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        }

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        waitShardAllAssigned(consumers, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumers, SHARD_COUNT));

        for (Consumer consumer : consumers) {
            RecordEntry recordEntry = consumer.read(10);
            Assert.assertNotNull(recordEntry);
        }

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        sleep(6000);

        Assert.assertTrue(getNewAssignment(consumers.get(SHARD_COUNT)).isEmpty());

        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testSessionTimeout() {
        String subId = createSubscription(tupleTopicName);

        List<Consumer> consumers = new ArrayList<>();

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        consumers.get(0).read(100);
        consumers.get(0).close();

        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        waitShardAllAssigned(consumers.subList(1, 3), SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumers.subList(1, 3), SHARD_COUNT));

        RecordEntry recordEntry = consumers.get(1).read(10);
        Assert.assertNotNull(recordEntry);
        recordEntry = consumers.get(2).read(10);
        Assert.assertNotNull(recordEntry);

        consumers.get(1).close();
        consumers.get(2).close();
        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        waitShardAllAssigned(consumers.get(3), SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumers.get(3), SHARD_COUNT));

        recordEntry = consumers.get(3).read(10);
        Assert.assertNotNull(recordEntry);
        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testReadEnd() {
        String subId = createSubscription(tupleTopicName);

        consumerConfig.setOffsetCommitTimeoutMs(10000);
        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);

        waitShardAllAssigned(consumer, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumer, SHARD_COUNT));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);
        sleep(5000);
        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");

        consumer.read(10);

        for (int i = 0; i < 50000; ++i) {
            RecordEntry recordEntry = consumer.read(3);
            if (recordEntry == null) {
                break;
            }
        }

        sleep(10000);
        Assert.assertNull(consumer.read(0)); // trigger commit
        sleep(2000);
        Assert.assertNull(consumer.read(0)); // trigger sync


        waitShardAllAssigned(consumer, SHARD_COUNT + 1);
        Assert.assertTrue(isAllAssigned(consumer, SHARD_COUNT + 1));
        consumer.close();
    }

    @Test
    public void testMultiReadEnd() {
        String subId = createSubscription(tupleTopicName);

        List<Consumer> consumers = new ArrayList<>();

        consumerConfig.setOffsetCommitTimeoutMs(10000);
        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));

        waitShardAllAssigned(consumers, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumers, SHARD_COUNT));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        sleep(5000);

        for (int i = 0; i < SHARD_COUNT; ++i) {
            client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "" + i);
        }

        Assert.assertNotNull(consumers.get(0).read(10));
        Assert.assertNotNull(consumers.get(1).read(10));

        for (int i = 0; i < 50000; ++i) {
            RecordEntry record1 = consumers.get(0).read(5);
            RecordEntry record2 = consumers.get(1).read(5);
            if (record1 == null && record2 == null) {
                break;
            }
        }

        Assert.assertNull(consumers.get(0).read(0));
        Assert.assertNull(consumers.get(1).read(0));

        sleep(10000);
        // trigger commit
        Assert.assertNull(consumers.get(0).read(0));
        Assert.assertNull(consumers.get(1).read(0));

        // trigger sync
        Assert.assertNull(consumers.get(0).read(0));
        Assert.assertNull(consumers.get(1).read(0));

        waitShardAllAssigned(consumers, SHARD_COUNT * 2);
        Assert.assertTrue(isAllAssigned(consumers, SHARD_COUNT * 2));

        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testAssignConsumeTuple() {
        String subId = createSubscription(tupleTopicName);
        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2"), consumerConfig);

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        RecordEntry recordEntry = consumer.read(10);
        Assert.assertNotNull(recordEntry);

        consumer.close();
    }

    @Test
    public void testConsumeSequence() {
        String subId = createSubscription(tupleTopicName);

        Map<String, SubscriptionOffset> offsets = client.openSubscriptionSession(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3")).getOffsets();
        for (SubscriptionOffset offset : offsets.values()) {
            Assert.assertEquals(-1, offset.getSequence());
            offset.setSequence(1);
            offset.setTimestamp(1);
        }

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        client.commitSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsets);

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2"), consumerConfig);

        RecordEntry recordEntry = consumer.read(10);
        Assert.assertEquals(2, recordEntry.getSequence());

        consumer.close();
    }

    @Test
    public void testTopicNotFound() {
        String subId = createSubscription(tupleTopicName);

        try {
            Consumer consumer = new Consumer(TEST_PROJECT_NAME, "topicNotFound", subId, consumerConfig);
            Assert.fail("throw exception");
        } catch (ResourceNotFoundException e) {
            Assert.assertEquals("The specified topic name does not exist.", e.getErrorMessage());
        }
    }

    @Test
    public void testSubscriptionNotFound() {
        try {
            Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, "subId", consumerConfig);
            Assert.fail("throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals("The specified subscription id invalid.", e.getErrorMessage());
        }
    }

    @Test
    public void testInvalidSequence() {
        String subId = createSubscription(tupleTopicName);

        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"));
        Map<String, SubscriptionOffset> offsets = openSubscriptionSessionResult.getOffsets();
        for (SubscriptionOffset offset : offsets.values()) {
            Assert.assertEquals("1", offset.getSessionId());
            offset.setSequence(1);
        }

        client.commitSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsets);

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"), consumerConfig);
        consumer.read(100);

        consumer.close();
    }

    @Test
    public void testInvalidSystemTime() {
        String subId = createSubscription(tupleTopicName);

        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"));
        Map<String, SubscriptionOffset> offsets = openSubscriptionSessionResult.getOffsets();
        for (SubscriptionOffset offset : offsets.values()) {
            Assert.assertEquals("1", offset.getSessionId());
            offset.setSequence(1);
            offset.setTimestamp(System.currentTimeMillis() * 1000 + 1);
        }

        client.commitSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsets);

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"), consumerConfig);

        try {
            consumer.read(100);
            Assert.fail("throw exception");
        } catch (InvalidParameterException e) {
            Assert.assertEquals("Time in seek request is out of range.", e.getErrorMessage());
        }

        consumer.close();
    }

    @Test
    public void testInvalidSessionId() {
        String subId = createSubscription(tupleTopicName);

        client.openSubscriptionSession(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"));

        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3"), consumerConfig);

        Map<String, SubscriptionOffset> offsets = client.openSubscriptionSession(TEST_PROJECT_NAME, tupleTopicName, subId, Arrays.asList("0", "1", "2", "3")).getOffsets();
        for (SubscriptionOffset offset : offsets.values()) {
            Assert.assertEquals("3", offset.getSessionId());
        }

        try {
            consumer.read((int) consumerConfig.getOffsetCommitTimeoutMs() / 1000 + 2);
            Assert.fail("throw exception");
        } catch (SubscriptionSessionInvalidException e) {
            Assert.assertEquals("Offset session has changed", e.getErrorMessage());
        }

        consumer.close();
    }

    @Test
    public void testConsumeInOrder() {
        String subId = createSubscription(tupleTopicName);

        Map<String, List<RecordEntry>> recordMap = new HashMap<>(SHARD_COUNT);

        List<Consumer> consumers = new ArrayList<>();

        produceTupleRecordsInOrder(tupleTopicName, schema, SHARD_COUNT);

        for (int i = 0; i < SHARD_COUNT / 3; ++i) {
            consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        }

        for (int i = 0; i < SHARD_COUNT / 3; ++i) {
            RecordEntry recordEntry = consumers.get(i).read(300);
            Assert.assertTrue(checkRecordOrder(recordMap, recordEntry));
        }

        for (int i = SHARD_COUNT / 3; i < SHARD_COUNT; ++i) {
            consumers.add(new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig));
        }

        waitShardRelease(consumers.subList(0, SHARD_COUNT / 3));

        for (int i = 0; i < SHARD_COUNT; ++i) {
            RecordEntry recordEntry = consumers.get(i).read(300);
            Assert.assertTrue(checkRecordOrder(recordMap, recordEntry));
        }

        for (int i = 0; i < SHARD_COUNT / 3; ++i) {
            consumers.get(i).close();
        }

        for (int i = SHARD_COUNT / 3; i < SHARD_COUNT; ++i) {
            RecordEntry recordEntry = consumers.get(i).read(300);
            Assert.assertTrue(checkRecordOrder(recordMap, recordEntry));
        }

        for (int i = 0; i < SHARD_COUNT; ++i) {
            consumers.get(i).close();
        }
    }

    @Test
    public void testConsumeInOrder2() {
        final String subId = createSubscription(tupleTopicName);

        final Map<String, List<RecordEntry>> recordMap = new HashMap<>(SHARD_COUNT);
        final Random random = new Random(System.currentTimeMillis());
        ExecutorService executor = new ThreadPoolExecutor(1, SHARD_COUNT,
                60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

        produceTupleRecordsInOrder(tupleTopicName, schema, SHARD_COUNT);

        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < SHARD_COUNT; ++i) {
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);
                    sleep(random.nextInt(10000));
                    for (int i = 0; i < random.nextInt(60); ++i) {
                        RecordEntry recordEntry = consumer.read(500);
                        synchronized (recordMap) {
                            if (!checkRecordOrder(recordMap, recordEntry)) {
                                consumer.close();
                                return false;
                            }
                        }
                        sleep(100);
                    }
                    consumer.close();
                    return true;
                }
            }));
        }

        for (int i = 0; i < SHARD_COUNT; ++i) {
            try {
                Assert.assertTrue(futures.get(i).get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        Assert.assertFalse(recordMap.isEmpty());
        executor.shutdownNow();
    }

    @Test
    public void testResetOffset() {
        String subId = createSubscription(tupleTopicName);

        consumerConfig.setOffsetCommitTimeoutMs(10000);
        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);

        produceTupleRecordsInOrder(tupleTopicName, schema, SHARD_COUNT);

        long sequence = -1;

        RecordEntry recordEntry = consumer.read(100);
        if (recordEntry != null && recordEntry.getShardId().equals("0")) {
            Assert.assertEquals(sequence + 1, recordEntry.getSequence());
            sequence = recordEntry.getSequence();
        }

        for (int i = 0; i < SHARD_COUNT * 1000; ++i) {
            recordEntry = consumer.read(3);
            if (recordEntry != null && recordEntry.getShardId().equals("0")) {
                Assert.assertEquals(sequence + 1, recordEntry.getSequence());
                sequence = recordEntry.getSequence();
            }
            if (recordEntry == null) {
                break;
            }
        }

        Assert.assertTrue(sequence > 1);

        SubscriptionOffset offset = new SubscriptionOffset();
        offset.setSequence(0);
        offset.setTimestamp(0);
        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", offset);
        client.resetSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsetMap);

        sleep(10000);
        try {
            consumer.read(3);
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }
        consumer.close();
    }

    @Test
    public void testReadEndResetOffset() {
        String subId = createSubscription(tupleTopicName);

        consumerConfig.setOffsetCommitTimeoutMs(10000);
        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);

        waitShardAllAssigned(consumer, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumer, SHARD_COUNT));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);
        sleep(5000);
        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");

        consumer.read(10);

        for (int i = 0; i < 50000; ++i) {
            RecordEntry recordEntry = consumer.read(5);
            if (recordEntry == null) {
                break;
            }
        }

        Assert.assertNull(consumer.read(0));
        sleep(10000);
        Assert.assertNull(consumer.read(0)); // ensure commit
        sleep(2000);
        Assert.assertNull(consumer.read(0)); // sync

        waitShardAllAssigned(consumer, SHARD_COUNT + 1);
        Assert.assertTrue(isAllAssigned(consumer, SHARD_COUNT + 1));

        SubscriptionOffset offset = new SubscriptionOffset();
        offset.setSequence(0);
        offset.setTimestamp(0);
        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", offset);
        client.resetSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsetMap);
        sleep(10000);

        try {
            consumer.read(3);
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }
        consumer.close();
    }

    @Test
    public void testResetOffset2() {
        String subId = createSubscription(tupleTopicName);

        consumerConfig.setOffsetCommitTimeoutMs(100000);
        Consumer consumer = new Consumer(TEST_PROJECT_NAME, tupleTopicName, subId, consumerConfig);

        waitShardAllAssigned(consumer, SHARD_COUNT);
        Assert.assertTrue(isAllAssigned(consumer, SHARD_COUNT));

        produceTupleRecords(tupleTopicName, schema, SHARD_COUNT);

        consumer.read(10);

        SubscriptionOffset offset = new SubscriptionOffset();
        offset.setSequence(0);
        offset.setTimestamp(0);
        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", offset);
        client.resetSubscriptionOffset(TEST_PROJECT_NAME, tupleTopicName, subId, offsetMap);
        sleep(1000);

        try {
            for (int i = 0; i < consumerConfig.getSessionTimeoutMs(); ++i) {
                consumer.read(3);
                sleep(1000);
            }
            Assert.fail("throw exception");
        } catch (SubscriptionOffsetResetException e) {
            Assert.assertEquals("Offset has reseted", e.getErrorMessage());
        }
        consumer.close();
    }
}
