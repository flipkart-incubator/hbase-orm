package com.flipkart.hbaseobjectmapper.codec;

import java.io.IOException;

/**
 * To be thrown when {@link Codec} fails to serialize
 */
public class SerializationException extends IOException {

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
