package com.flipkart.hbaseobjectmapper.exceptions;

public class EmptyConstructorInaccessibleException extends IllegalArgumentException {
    public EmptyConstructorInaccessibleException(String s) {
        super(s);
    }

    public EmptyConstructorInaccessibleException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
