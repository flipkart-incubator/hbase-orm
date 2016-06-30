package com.flipkart.hbaseobjectmapper.exceptions;

public class InternalError extends Error {
    public InternalError(Throwable cause) {
        super("Internal error - possibly some assumptions in library are invalid", cause);
    }
}
