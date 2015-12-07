package com.flipkart.hbaseobjectmapper.exceptions;

import java.lang.reflect.Field;

public class MappedColumnCantBeTransientException extends IllegalArgumentException {
    public MappedColumnCantBeTransientException(Field field, String hbColumnName) {
        super(String.format("In class \"%s\", the field \"%s\" is annotated with \"%s\", but is declared as transient (Transient fields cannot be persisted)", field.getDeclaringClass().getName(), field.getName(), hbColumnName));
    }
}
