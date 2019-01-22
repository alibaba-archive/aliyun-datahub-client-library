package com.aliyun.datahub.clientlibrary.example;

import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerExample {
    private static final String TEST_ENDPOINT = "**datahub endpoint**";
    private static final String TEST_PROJECT = "** datahub project **";
    private static final String TEST_TOPIC = "** datahub tuple topic **";
    private static final String TEST_SUB_ID = "** subscription id **";
    private static final String TEST_AK = "** access id **";
    private static final String TEST_SK = "** access key **";
    private static final List<String> TEST_ASSIGNMENT = Arrays.asList("0", "1", "2");

    public static void consumeAutoAssigned() {
        ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, config);
        boolean stop = false;
        while (!stop) {
            try {
                consume(consumer);
            } catch (SubscriptionSessionInvalidException | SubscriptionOffsetResetException e) {
                // - subscription exception, will not recover
                // print some log or just use a new consumer
                consumer.close();
                consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, config);
            } catch (ResourceNotFoundException | InvalidParameterException e) {
                // - project, topic, shard, subscription not found
                // - seek out of range
                // - sometimes shard operation cause ResourceNotFoundException
                // should make sure if resource exists, print some log or just exit
            } catch (DatahubClientException e) {
                // - network or other exception exceed retry limit
                // can just sleep and retry
            }
        }
        consumer.close();
    }

    public static void consumeByShards() {
        ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, TEST_ASSIGNMENT, config);

        boolean stop = false;
        while (!stop) {
            try {
                consume(consumer);
            } catch (SubscriptionSessionInvalidException | SubscriptionOffsetResetException e) {
                // - subscription exception, will not recover
                // print some log or just use a new consumer
                consumer.close();
                consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, TEST_ASSIGNMENT, config);
            } catch (ResourceNotFoundException | InvalidParameterException e) {
                // - project, topic, shard, subscription not found
                // - seek out of range
                // - sometimes shard operation cause ResourceNotFoundException
                // should make sure if resource exists, print some log or just exit
            } catch (DatahubClientException e) {
                // - network or other exception exceed retry limit
                // can make some retry
            }
        }
        consumer.close();
    }

    public static void consumeByOffset() {
        ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 10000));
        offsetMap.put("1", new Offset(1, 10000));
        offsetMap.put("2", new Offset(1, 10000));

        Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, offsetMap, config);

        boolean stop = false;
        while (!stop) {
            try {
                consume(consumer);
            } catch (SubscriptionSessionInvalidException | SubscriptionOffsetResetException e) {
                // - subscription exception, will not recover
                // print some log or just use a new consumer
                consumer.close();
                consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, offsetMap, config);
            } catch (ResourceNotFoundException | InvalidParameterException e) {
                // - project, topic, shard, subscription not found
                // - seek out of range
                // - sometimes shard operation cause ResourceNotFoundException
                // should make sure if resource exists, print some log or just exit
            } catch (DatahubClientException e) {
                // - network or other exception exceed retry limit
                // can make some retry
            }
        }
        consumer.close();
    }

    private static void consume(Consumer consumer) {
        int maxRetry = 3;
        while (true) {
            RecordEntry record = consumer.read(maxRetry);
            if (record != null) {
                // process
                TupleRecordData data = (TupleRecordData) record.getRecordData();
                System.out.println("field1:" + data.getField(0) + ", field2:" + data.getField("field2"));
            }
        }
    }

    public static void main(String[] args) {
        // auto assigned
        consumeAutoAssigned();

        // manually assigned shards
        consumeByShards();

        // manually assigned offset
        consumeByOffset();
    }
}
