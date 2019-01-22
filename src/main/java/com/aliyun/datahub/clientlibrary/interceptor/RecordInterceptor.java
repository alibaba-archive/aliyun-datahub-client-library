package com.aliyun.datahub.clientlibrary.interceptor;

import com.aliyun.datahub.client.model.RecordEntry;

import java.util.List;

public interface RecordInterceptor {
    List<RecordEntry> beforeWrite(List<RecordEntry> records);
    List<RecordEntry> afterRead(List<RecordEntry> records);
}
