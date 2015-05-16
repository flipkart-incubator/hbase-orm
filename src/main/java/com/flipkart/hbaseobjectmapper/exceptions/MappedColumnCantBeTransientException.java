package com.flipkart.hbaseobjectmapper.exceptions;

public class MappedColumnCantBeTransientException extends IllegalArgumentException {
    public MappedColumnCantBeTransientException(String s) {
        super(s);
    }

    public MappedColumnCantBeTransientException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
