package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@HBTable(name = "blah", families = {@Family(name = "f")})
public class ClassWithTwoHBColumnAnnotations implements HBRecord<String> {

    protected String key = "key";

    @HBColumn(family = "f", column = "c1")
    @HBColumnMultiVersion(family = "f", column = "c1")
    private Float c1;

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }
}
