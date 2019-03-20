# DataHub Client Library

High level api for DataHub SDK. If shard id not given, Producer will pick a shard by Round Robin algorithm. Consuming data by Consumer, shards can be assigned by server, and return the record with as early timestamp as possible.

# Example

| 参数列表 | Value |
| ---- | ---- |
| DATAHUB_ENDPOINT | DataHub server endpoint |
| TEST_PROJECT | Project name |
| TEST_TOPIC | Topic name |
| TEST_SHARD | Shard ID |
| TEST_SUB_ID | Subscription ID |
| TEST_AK | Your access id |
| TEST_SK | Your access key |

### Maven

    <dependency>
        <groupId>com.aliyun.datahub</groupId>
        <artifactId>datahub-client-library</artifactId>
        <version>1.0.4-public</version>
    </dependency>
    
**1. Init Producer**

    ProducerConfig config = new ProducerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
    Producer producer = new Producer(TEST_PROJECT, TEST_TOPIC, config);

**2. Upload data**

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.STRING));
    schema.addField(new Field("field2", FieldType.BIGINT));
    
    List<RecordEntry> recordEntries = new ArrayList<>();
    for (int cnt = 0; cnt < 10; ++cnt) {
        RecordEntry entry = new RecordEntry();
        entry.addAttribute("key1", "value1");
        entry.addAttribute("key2", "value2");
        
        TupleRecordData data = new TupleRecordData(schema);
        data.setField("field1", "testValue");
        data.setField("field2", 1);
        
        entry.setRecordData(data);
        recordEntries.add(entry);
    }
    
    int maxRetry = 3;
    while (true) {
        try {
            producer.send(records, maxRetry);
            break;
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
    
    // close before exit
    producer.close();

**3. Init Consumer**

    ConsumerConfig config = new ConsumerConfig(TEST_ENDPOINT, TEST_AK, TEST_SK);
    Consumer consumer = new Consumer(TEST_PROJECT, TEST_TOPIC, TEST_SUB_ID, config);
    
**4. Consume data**

    int maxRetry = 3;
    boolean stop = false;
    while (!stop) {
        try {
            while (true) {
                RecordEntry record = consumer.read(maxRetry);
                if (record != null) {
                    TupleRecordData data = (TupleRecordData) record.getRecordData();
                    System.out.println("field1:" + data.getField(0) + ", field2:" + data.getField("field2"));
                }
            }
        } catch (SubscriptionSessionInvalidException | SubscriptionOffsetResetException e) {
            // subscription exception, will not recover
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
    
    // close before exit
    consumer.close();

[more example](./src/main/java/com/aliyun/datahub/clientlibrary/example)

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
