package com.flipkart.hbaseobjectmapper.exceptions;

public class UnsupportedFieldTypeException extends IllegalArgumentException {
    public UnsupportedFieldTypeException(String s) {
        super(s);
    }

    public UnsupportedFieldTypeException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
