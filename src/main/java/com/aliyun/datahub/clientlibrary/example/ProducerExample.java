package com.aliyun.datahub.clientlibrary.example;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.producer.Producer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProducerExample {
    private static final String TEST_ENDPOINT = "**datahub endpoint**";
    private static final String TEST_PROJECT = "** datahub project **";
    private static final String TEST_TOPIC = "** datahub tuple topic **";
    private static final String TEST_AK = "** access id **";
    private static final String TEST_SK = "** access key **";
    private static final List<String> TEST_ASSIGNMENT = Arrays.asList("0", "1", "2");

    public static Producer autoAssignedProducer() {
        ProducerConfig config = new ProducerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        return new Producer(TEST_PROJECT, TEST_TOPIC, config);
    }

    public static Producer manualAssignedProducer() {
        ProducerConfig config = new ProducerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        return new Producer(TEST_PROJECT, TEST_TOPIC, TEST_ASSIGNMENT, config);
    }

    public static void produce(Producer producer) {
        RecordSchema schema = getSchema(); // call only once

        List<RecordEntry> records = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            records.add(genTupleData(schema));
        }

        int maxRetry = 3;
        while (true) {
            try {
                producer.send(records, maxRetry);
            } catch (MalformedRecordException e) {
                // malformed RecordEntry
            } catch (InvalidParameterException e) {
                // invalid param
            } catch (ResourceNotFoundException e) {
                // project, topic or shard not found, sometimes caused by split/merge shard
            } catch (DatahubClientException e) {
                // network or other exceptions exceeded retry limit
            }
        }
    }

    public static RecordSchema getSchema() {
        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(new DatahubConfig(TEST_ENDPOINT, new AliyunAccount(TEST_AK, TEST_SK))).build();
        return client.getTopic(TEST_PROJECT, TEST_TOPIC).getRecordSchema();
    }

    private static RecordEntry genTupleData(RecordSchema schema) {
        final TupleRecordData data = new TupleRecordData(schema);
        for (Field field : schema.getFields()) {
            switch (field.getType()) {
                case STRING:
                    data.setField(field.getName(), "string");
                    break;
                case BIGINT:
                    data.setField(field.getName(), 5L);
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
        }};
    }

    public static void main(String[] args) {
        Producer producer = manualAssignedProducer();
        produce(producer);
        producer.close();

        producer = autoAssignedProducer();
        produce(producer);
        producer.close();
    }
}
