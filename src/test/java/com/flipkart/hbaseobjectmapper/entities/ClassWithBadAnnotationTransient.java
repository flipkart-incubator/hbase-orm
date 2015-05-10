package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;

public class ClassWithBadAnnotationTransient extends HBRecordTestClass {
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
