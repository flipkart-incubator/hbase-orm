package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
@HBTable(name = "blah", families = {@Family(name = "a")})
public class ClassWithBadAnnotationTransient implements HBRecord<String> {
    @HBRowKey
    protected String key = "key";

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    @HBColumn(family = "a", column = "first_name")
    private String firstName;
    @HBColumn(family = "a", column = "last_name")
    private String lastName;
    @HBColumn(family = "a", column = "full_name")
    private transient String fullName; // not allowed

    public ClassWithBadAnnotationTransient() {
    }

    public ClassWithBadAnnotationTransient(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.fullName = firstName + " " + lastName;
    }
}
