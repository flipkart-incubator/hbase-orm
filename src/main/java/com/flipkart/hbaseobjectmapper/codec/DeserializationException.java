package com.flipkart.hbaseobjectmapper.codec;

import java.io.IOException;

/**
 * To be thrown when {@link Codec} fails to deserialize
 */
public class DeserializationException extends IOException {
    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
