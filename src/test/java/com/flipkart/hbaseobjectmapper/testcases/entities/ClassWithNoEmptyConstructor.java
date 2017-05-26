package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings({"FieldCanBeLocal", "CanBeFinal", "unused"})
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithNoEmptyConstructor implements HBRecord<String> {
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
    private Integer i;

    public ClassWithNoEmptyConstructor(int i) {
        this.i = i;
    }
}
