package com.flipkart.hbaseobjectmapper.exceptions;

import java.lang.annotation.Annotation;

public class DuplicateCodecFlagForColumnException extends IllegalArgumentException {
    public DuplicateCodecFlagForColumnException(Class<?> recordClass, String fieldName, Class<? extends Annotation> annotationClass, String flagName) {
        super(String.format("The @%s annotation on field %s on class %s has duplicate codec flags (See flag %s)", annotationClass, fieldName, recordClass, flagName));
    }
}
