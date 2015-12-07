package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;

import java.util.NavigableMap;

public class IncompatibleFieldForHBColumnMultiVersionAnnotationException extends IllegalArgumentException {
    public IncompatibleFieldForHBColumnMultiVersionAnnotationException(String message) {
        super(String.format("A field annotated with @%s should be of type %s<%s, ?> (%s)", HBColumnMultiVersion.class.getName(), NavigableMap.class.getName(), Long.class.getName(), message));
    }
}
