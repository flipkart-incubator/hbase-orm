package com.flipkart.hbaseobjectmapper.exceptions;

public class MissingHBColumnFieldsException extends IllegalArgumentException {
    public MissingHBColumnFieldsException(String s) {
        super(s);
    }

    public MissingHBColumnFieldsException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
