package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class Singleton extends HBRecordTestClass {
    private static Singleton ourInstance = new Singleton();

    @HBColumn(family = "f", column = "c")
    String column;

    public static Singleton getInstance() {
        return ourInstance;
    }

    private Singleton() {
        column = "something";
    }

}
