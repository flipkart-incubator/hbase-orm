package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBRecord;

public class HBRecordTestClass implements HBRecord {
    protected String key = "key";

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }
}
