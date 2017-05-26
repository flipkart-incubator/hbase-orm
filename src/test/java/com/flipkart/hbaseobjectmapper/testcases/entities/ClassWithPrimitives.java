package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithPrimitives implements HBRecord<String> {
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
    private float f;

    public ClassWithPrimitives() {

    }

    public ClassWithPrimitives(float f) {
        this.f = f;
    }
}
