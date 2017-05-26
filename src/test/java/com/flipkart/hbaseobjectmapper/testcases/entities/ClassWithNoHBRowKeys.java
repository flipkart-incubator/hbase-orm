package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;

@SuppressWarnings("unused")
@HBTable(name = "blah", families = {@Family(name = "f")})
public class ClassWithNoHBRowKeys implements HBRecord<String> {
    protected String key = "key";

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    @HBColumn(family = "f", column = "c")
    private Float f;
}
