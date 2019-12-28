package com.flipkart.hbaseobjectmapper.exceptions;

public class NoEmptyConstructorException extends IllegalArgumentException {

    public NoEmptyConstructorException(Class<?> clazz, Throwable throwable) {
        super(String.format("Class %s needs to specify an empty (public) constructor", clazz.getName()), throwable);
    }
}
