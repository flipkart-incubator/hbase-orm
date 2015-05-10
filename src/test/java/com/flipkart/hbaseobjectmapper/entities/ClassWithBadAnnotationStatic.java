package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithBadAnnotationStatic extends HBRecordTestClass {
    @HBColumn(family = "a", column = "num_months")
    private static Integer NUM_MONTHS = 12; // not allowed
}
