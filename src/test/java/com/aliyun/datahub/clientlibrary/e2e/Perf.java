package com.aliyun.datahub.clientlibrary.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import com.aliyun.datahub.clientlibrary.e2e.common.Configure;
import com.aliyun.datahub.clientlibrary.e2e.common.Constant;
import com.aliyun.datahub.clientlibrary.producer.Producer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Perf {
    private static final String TEST_ENDPOINT = Configure.getString(Constant.DATAHUB_ENDPOINT);
    private static final String TEST_PROJECT = "1111";
    private static final String TEST_TOPIC = "perf";
    private static final String TEST_AK = Configure.getString(Constant.DATAHUB_ACCESS_ID);
    private static final String TEST_SK = Configure.getString(Constant.DATAHUB_ACCESS_KEY);
    private static final DatahubConfig datahubConfig = new DatahubConfig(TEST_ENDPOINT, new AliyunAccount(TEST_AK, TEST_SK), true);
    private static final DatahubClient client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();

    private static RecordEntry genTupleData(RecordSchema schema) {
        final TupleRecordData data = new TupleRecordData(schema);
        for (Field field : schema.getFields()) {
            switch (field.getType()) {
                case STRING:
                    data.setField(field.getName(), "string");
                    break;
                case BIGINT:
                    data.setField(field.getName(), 1);
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

    static List<RecordEntry> genTupleRecords(RecordSchema schema) {
        List<RecordEntry> result = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            result.add(genTupleData(schema));
        }
        return result;
    }

    static void produce() {
        GetTopicResult result = client.getTopic(TEST_PROJECT, TEST_TOPIC);
        final RecordSchema schema = result.getRecordSchema();
        int shardCount = result.getShardCount();
        final Producer producer = new Producer(TEST_PROJECT, TEST_TOPIC, new ProducerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK));
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < shardCount; ++i) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; ++i) {
                        producer.send(genTupleRecords(schema), 3);
                        System.out.println(Thread.currentThread().getId() + " producing, round: " + i);
                    }
                }
            }));
            threads.get(threads.size() - 1).start();
        }
        for (int i = 0; i < shardCount; ++i) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        //produce();
        String subId = client.createSubscription(TEST_PROJECT, TEST_TOPIC, "comment").getSubId();

        ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
        config.setFetchSize(1000);
        config.setAutoCommit(false);

        List<String> shardIds = new ArrayList<>();
        for (int i = 0; i < 256; ++i) {
            shardIds.add("" + i);
        }

        Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, subId, config);
        // Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, subId, shardIds, config);

        long start = System.currentTimeMillis();
        long total = 0;
        for (int i = 0; i < 100000; ++i) {
            RecordEntry RecordEntry = consumer.read(1000);
            if (RecordEntry == null) {
                continue;
            }
            ++total;
            System.out.println(i);
        }
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));
        System.out.println("records: " + total);
        System.out.println("rps: " + (total * 1000 / (end - start)));
        consumer.close();
        client.deleteSubscription(TEST_PROJECT, TEST_TOPIC, subId);
    }
}
