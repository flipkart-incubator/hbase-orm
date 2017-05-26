package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings("unused")
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithTwoFieldsMappedToSameColumn implements HBRecord<String> {
    @HBRowKey
    protected String key = "key";

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    @HBColumn(family = "a", column = "b")
    private Integer i = 1;
    @HBColumn(family = "a", column = "b")
    private Integer j = 2;

}
