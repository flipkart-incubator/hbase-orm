package com.flipkart.hbaseobjectmapper.codec;

import java.io.IOException;

public class SerializationException extends IOException {

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
