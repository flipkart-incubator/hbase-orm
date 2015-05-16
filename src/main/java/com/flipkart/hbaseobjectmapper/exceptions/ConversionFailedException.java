package com.flipkart.hbaseobjectmapper.exceptions;

/**
 * Exception raised due to an unhandled scenario in {@link com.flipkart.hbaseobjectmapper.HBObjectMapper HBObjectMapper}
 */
public class ConversionFailedException extends RuntimeException {
    public ConversionFailedException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConversionFailedException(String s) {
        super(s);
    }
}
