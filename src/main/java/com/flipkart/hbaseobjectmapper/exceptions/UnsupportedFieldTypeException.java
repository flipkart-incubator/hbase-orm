package com.flipkart.hbaseobjectmapper.exceptions;

public class UnsupportedFieldTypeException extends IllegalArgumentException {
    public UnsupportedFieldTypeException(String s) {
        super(s);
    }
}
