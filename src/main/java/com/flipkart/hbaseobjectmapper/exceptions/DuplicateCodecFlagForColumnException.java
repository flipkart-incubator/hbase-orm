package com.flipkart.hbaseobjectmapper.exceptions;

public class DuplicateCodecFlagForColumnException extends IllegalArgumentException {
    public DuplicateCodecFlagForColumnException(Class recordClass, String fieldName, Class annotationClass, String flagName) {
        super(String.format("The @%s annotation on field %s on class %s has duplicate codec flags (See flag %s)", annotationClass, fieldName, recordClass, flagName));
    }
}
