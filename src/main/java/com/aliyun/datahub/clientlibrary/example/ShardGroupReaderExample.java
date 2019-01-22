package com.aliyun.datahub.clientlibrary.example;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.clientlibrary.consumer.ShardGroupReader;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.models.Offset;

import java.util.HashMap;
import java.util.Map;

public class ShardGroupReaderExample {
    private static final String TEST_ENDPOINT = "**datahub endpoint**";
    private static final String TEST_PROJECT = "** datahub project **";
    private static final String TEST_TOPIC = "** datahub tuple topic **";
    private static final String TEST_AK = "** access id **";
    private static final String TEST_SK = "** access key **";

    public static ShardGroupReader getSharedGroupReader() {
        ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        ShardGroupReader shardGroupReader = new ShardGroupReader(TEST_PROJECT, TEST_TOPIC, config);
        Map<String, Offset> offsetMap = new HashMap<>();
        offsetMap.put("0", new Offset(1, 1));
        offsetMap.put("1", new Offset(1, 1));
        offsetMap.put("2", new Offset(1, 1));
        shardGroupReader.createShardReader(offsetMap);
        return shardGroupReader;
    }

    public static void consume(ShardGroupReader shardGroupReader) {
        try {
            while (true) {
                RecordEntry record = shardGroupReader.read();
                if (record != null) {
                    // process
                    TupleRecordData data = (TupleRecordData) record.getRecordData();
                    System.out.println("field1:" + data.getField(0) + ", field2:" + data.getField("field2"));
                }
            }
        } catch (ResourceNotFoundException e) {
            // project, topic or shard not found, sometimes caused by split/merge shard
        } catch (InvalidParameterException e) {
            // invalid param
        } catch (DatahubClientException e) {
            // network or other exception exceed retry limit
        }
    }

    public static void main(String[] args) {
        ShardGroupReader shardGroupReader = getSharedGroupReader();
        consume(shardGroupReader);
        shardGroupReader.close();
    }
}
