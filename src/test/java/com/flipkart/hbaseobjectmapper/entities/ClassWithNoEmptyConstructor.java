package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithNoEmptyConstructor extends HBRecordTestClass {
    @HBColumn(family = "a", column = "b")
    private Integer i;

    public ClassWithNoEmptyConstructor(int i) {
        this.i = i;
    }
}
