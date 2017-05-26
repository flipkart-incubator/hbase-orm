package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings("unused")
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithBadAnnotationStatic implements HBRecord<String> {
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

    @HBColumn(family = "a", column = "num_months")
    private static Integer NUM_MONTHS = 12; // not allowed
}
