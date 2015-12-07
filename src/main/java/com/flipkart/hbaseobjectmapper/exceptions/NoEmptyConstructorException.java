package com.flipkart.hbaseobjectmapper.exceptions;

public class NoEmptyConstructorException extends IllegalArgumentException {

    public NoEmptyConstructorException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
