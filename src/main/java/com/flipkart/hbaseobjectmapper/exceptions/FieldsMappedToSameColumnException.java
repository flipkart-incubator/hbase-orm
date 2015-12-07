package com.flipkart.hbaseobjectmapper.exceptions;

public class FieldsMappedToSameColumnException extends IllegalArgumentException {
    public FieldsMappedToSameColumnException(String s) {
        super(s);
    }
}
