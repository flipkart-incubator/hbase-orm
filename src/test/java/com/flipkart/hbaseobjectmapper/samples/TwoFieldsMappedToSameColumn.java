package com.flipkart.hbaseobjectmapper.samples;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;

public class TwoFieldsMappedToSameColumn implements HBRecord {
    @HBColumn(family = "a", column = "b")
    private Integer i = 1;
    @HBColumn(family = "a", column = "b")
    private Integer j = 2;

    @Override
    public String composeRowKey() {
        return "a";
    }

    @Override
    public void parseRowKey(String rowKey) {

    }
}
