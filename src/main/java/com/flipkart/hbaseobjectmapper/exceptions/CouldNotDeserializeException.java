package com.flipkart.hbaseobjectmapper.exceptions;

public class CouldNotDeserializeException extends IllegalStateException {
    public CouldNotDeserializeException(Throwable e) {
        super(e);
    }
}
