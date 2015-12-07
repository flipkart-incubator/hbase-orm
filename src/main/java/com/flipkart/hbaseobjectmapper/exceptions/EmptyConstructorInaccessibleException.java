package com.flipkart.hbaseobjectmapper.exceptions;

public class EmptyConstructorInaccessibleException extends IllegalArgumentException {
    public EmptyConstructorInaccessibleException(String s) {
        super(s);
    }
}
