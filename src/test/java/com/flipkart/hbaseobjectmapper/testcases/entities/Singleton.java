package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings({"CanBeFinal", "unused"})
@HBTable(name = "blah", families = {@Family(name = "f")})
public class Singleton implements HBRecord<String> {
    private static Singleton ourInstance = new Singleton();

    @HBRowKey
    protected String key = "key";

    @HBColumn(family = "f", column = "c")
    String column;

    public static Singleton getInstance() {
        return ourInstance;
    }

    private Singleton() {
        column = "something";
    }

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

}
