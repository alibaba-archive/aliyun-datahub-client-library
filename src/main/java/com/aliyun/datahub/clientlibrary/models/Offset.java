package com.aliyun.datahub.clientlibrary.models;

import com.aliyun.datahub.client.model.SubscriptionOffset;

public class Offset {
    private long sequence;
    private long timestamp;

    public Offset() {
        this(-1, -1);
    }

    public Offset(long sequence, long timestamp) {
        this.sequence = sequence;
        this.timestamp = timestamp;
    }

    public Offset(SubscriptionOffset subscriptionOffset) {
        this.sequence = subscriptionOffset.getSequence();
        this.timestamp = subscriptionOffset.getTimestamp();
    }

    public long getSequence() {
        return sequence;
    }

    public Offset setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Offset setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public boolean isInvalid() {
        return sequence < 0 && timestamp < 0;
    }

    public boolean hasSequence() {
        return sequence >= 0;
    }

    public boolean hasTimestamp() {
        return timestamp >= 0;
    }
}
