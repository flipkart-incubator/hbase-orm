package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;

@SuppressWarnings("unused")
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithNoHBColumns implements HBRecord<String> {
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

    private Float f; //not adding @HBColumn here!
}
