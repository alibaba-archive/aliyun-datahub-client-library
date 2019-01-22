package com.aliyun.datahub.clientlibrary.interceptor;

import com.aliyun.datahub.client.model.RecordEntry;

import java.util.List;

public class EmptyInterceptor implements RecordInterceptor {
    public static EmptyInterceptor emptyInterceptor = new EmptyInterceptor();

    @Override
    public List<RecordEntry> beforeWrite(List<RecordEntry> records) {
        return records;
    }

    @Override
    public List<RecordEntry> afterRead(List<RecordEntry> records) {
        return records;
    }
}
