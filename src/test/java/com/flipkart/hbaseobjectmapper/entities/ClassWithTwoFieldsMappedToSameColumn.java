package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithTwoFieldsMappedToSameColumn extends HBRecordTestClass {
    @HBColumn(family = "a", column = "b")
    private Integer i = 1;
    @HBColumn(family = "a", column = "b")
    private Integer j = 2;

}
