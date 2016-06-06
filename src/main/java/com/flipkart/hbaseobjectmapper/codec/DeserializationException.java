package com.flipkart.hbaseobjectmapper.codec;

import java.io.IOException;

public class DeserializationException extends IOException {
    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
