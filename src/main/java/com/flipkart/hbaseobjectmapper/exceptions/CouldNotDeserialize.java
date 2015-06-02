package com.flipkart.hbaseobjectmapper.exceptions;

public class CouldNotDeserialize extends IllegalStateException {
    public CouldNotDeserialize(Throwable e) {
        super(e);
    }
}
