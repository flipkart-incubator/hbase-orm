package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;

import java.lang.reflect.Field;

public class BothHBColumnAnnotationsPresentException extends IllegalArgumentException {

    public BothHBColumnAnnotationsPresentException(Field field) {
        super(String.format("Class %s has a field %s that's annotated with both @%s and @%s (you can use only one of them on a field)", field.getDeclaringClass(), field.getName(), HBColumn.class.getName(), HBColumnMultiVersion.class.getName()));
    }
}
