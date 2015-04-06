package com.flipkart.hbaseobjectmapper.samples;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;

public class AllFieldsEmpty implements HBRecord {
    @HBColumn(family = "f", column = "a")
    private Integer a;
    @HBColumn(family = "f", column = "b")
    private Integer b;

    @Override
    public String composeRowKey() {
        return "a";
    }

    @Override
    public void parseRowKey(String rowKey) {

    }
}
