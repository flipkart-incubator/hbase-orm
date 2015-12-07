package com.flipkart.hbaseobjectmapper.exceptions;

public class RowKeyCantBeComposedException extends IllegalArgumentException {
    public RowKeyCantBeComposedException(Throwable throwable) {
        super("Error while composing row key for object", throwable);
    }
}
