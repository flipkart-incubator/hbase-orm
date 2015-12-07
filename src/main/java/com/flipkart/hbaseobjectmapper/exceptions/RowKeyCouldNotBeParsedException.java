package com.flipkart.hbaseobjectmapper.exceptions;

public class RowKeyCouldNotBeParsedException extends IllegalArgumentException {
    public RowKeyCouldNotBeParsedException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
