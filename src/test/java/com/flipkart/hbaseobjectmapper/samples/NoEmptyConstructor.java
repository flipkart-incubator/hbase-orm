package com.flipkart.hbaseobjectmapper.samples;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;

public class NoEmptyConstructor implements HBRecord {
    @HBColumn(family = "a", column = "b")
    private Integer i;

    public NoEmptyConstructor(int i) {
        this.i = i;
    }

    @Override
    public String composeRowKey() {
        return "a";
    }

    @Override
    public void parseRowKey(String rowKey) {

    }
}
