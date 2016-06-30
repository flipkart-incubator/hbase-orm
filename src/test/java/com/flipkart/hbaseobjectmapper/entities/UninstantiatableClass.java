package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class UninstantiatableClass implements HBRecord<String> {
    @HBRowKey
    private Integer uid;
    @HBColumn(family = "main", column = "name")
    private String name;

    public UninstantiatableClass() {
        throw new RuntimeException("I'm a bad constructor");
    }

    @Override
    public String composeRowKey() {
        return uid.toString();
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.uid = Integer.valueOf(rowKey);
    }
}
