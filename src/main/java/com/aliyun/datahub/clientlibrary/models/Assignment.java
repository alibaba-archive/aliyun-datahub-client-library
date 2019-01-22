package com.aliyun.datahub.clientlibrary.models;

import java.util.ArrayList;
import java.util.List;

public class Assignment {
    public static Assignment emptyAssignment = new Assignment();

    private List<String> newShardList;
    private List<String> releaseShardList;

    public Assignment() {
        newShardList = new ArrayList<>();
        releaseShardList = new ArrayList<>();
    }

    public Assignment(List<String> newShardList, List<String> releaseShardList) {
        this.newShardList = newShardList;
        this.releaseShardList = releaseShardList;
    }

    public boolean isEmpty() {
        return newShardList.isEmpty() && releaseShardList.isEmpty();
    }

    public List<String> getNewShardList() {
        return newShardList;
    }

    public void setNewShardList(List<String> newShardList) {
        this.newShardList = newShardList;
    }

    public List<String> getReleaseShardList() {
        return releaseShardList;
    }

    public void setReleaseShardList(List<String> releaseShardList) {
        this.releaseShardList = releaseShardList;
    }
}
