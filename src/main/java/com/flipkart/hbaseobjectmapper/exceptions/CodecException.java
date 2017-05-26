package com.flipkart.hbaseobjectmapper.exceptions;

public class CodecException extends IllegalArgumentException {
    public CodecException(String s, Throwable t) {
        super(s, t);
    }
}
