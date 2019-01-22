package com.aliyun.datahub.clientlibrary.models;

import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;

public class TopicInfo {
    private String projectName;
    private String topicName;
    private RecordType recordType;
    private RecordSchema recordSchema;

    public TopicInfo(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.recordType = RecordType.BLOB;
    }

    public TopicInfo(String projectName, String topicName, RecordSchema recordSchema) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.recordType = RecordType.TUPLE;
        this.recordSchema = recordSchema;
    }

    public TopicInfo(GetTopicResult getTopicResult) {
        this.projectName = getTopicResult.getProjectName();
        this.topicName = getTopicResult.getTopicName();
        this.recordType = getTopicResult.getRecordType();
        this.recordSchema = getTopicResult.getRecordSchema();
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public void setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }
}
