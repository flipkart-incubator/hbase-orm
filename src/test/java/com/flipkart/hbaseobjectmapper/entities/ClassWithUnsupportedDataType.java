package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

import java.util.Date;

public class ClassWithUnsupportedDataType extends HBRecordTestClass {
    @HBColumn(family = "f", column = "c")
    private Date date;
}
