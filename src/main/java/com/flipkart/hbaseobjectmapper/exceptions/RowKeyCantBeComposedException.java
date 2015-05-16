package com.flipkart.hbaseobjectmapper.exceptions;

public class RowKeyCantBeComposedException extends IllegalArgumentException {
    public RowKeyCantBeComposedException(String s) {
        super(s);
    }

    public RowKeyCantBeComposedException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
