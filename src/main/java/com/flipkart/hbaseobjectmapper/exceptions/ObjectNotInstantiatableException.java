package com.flipkart.hbaseobjectmapper.exceptions;

public class ObjectNotInstantiatableException extends IllegalArgumentException {
    public ObjectNotInstantiatableException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
