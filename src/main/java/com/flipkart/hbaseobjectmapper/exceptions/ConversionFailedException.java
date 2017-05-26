package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;

/**
 * Exception raised due to an unhandled scenario in {@link HBObjectMapper HBObjectMapper}
 */
public class ConversionFailedException extends RuntimeException {
    public ConversionFailedException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConversionFailedException(String s) {
        super(s);
    }
}
