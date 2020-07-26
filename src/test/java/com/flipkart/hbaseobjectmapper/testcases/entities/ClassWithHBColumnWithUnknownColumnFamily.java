package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@HBTable(name = "blah", families = {@Family(name = "f2")})
public class ClassWithHBColumnWithUnknownColumnFamily implements HBRecord<String> {

    private String key;

    @HBColumn(family = "f1", column = "c1")
    private Float c1;

    @HBColumn(family = "f1", column = "c1")
    private Double c2;

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }
}
