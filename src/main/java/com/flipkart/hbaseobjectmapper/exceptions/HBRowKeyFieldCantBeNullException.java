package com.flipkart.hbaseobjectmapper.exceptions;

public class HBRowKeyFieldCantBeNullException extends IllegalArgumentException {
    public HBRowKeyFieldCantBeNullException(String s) {
        super(s);
    }
}