package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithPrimitives extends HBRecordTestClass {
    @HBColumn(family = "a", column = "b")
    private float i;

    public ClassWithPrimitives() {
        
    }

    public ClassWithPrimitives(float i) {
        this.i = i;
    }
}
