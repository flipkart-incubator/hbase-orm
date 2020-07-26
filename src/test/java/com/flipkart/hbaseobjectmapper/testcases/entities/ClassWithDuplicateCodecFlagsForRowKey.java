package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@HBTable(name = "blah", families = {@Family(name = "f1")}, rowKeyCodecFlags = {
        @Flag(name = "flag1", value = "flagValue1"),
        @Flag(name = "flag1", value = "flagValue2")
})
public class ClassWithDuplicateCodecFlagsForRowKey implements HBRecord<String> {

    private String key;

    @HBColumn(family = "f1", column = "c1")
    private Float f1;

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = key;
    }
}
