package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithNoHBRowKeys extends HBRecordTestClass {
    @HBColumn(family = "f", column = "c")
    private Float f;
}
