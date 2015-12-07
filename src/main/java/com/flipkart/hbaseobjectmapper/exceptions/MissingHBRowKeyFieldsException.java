package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBRowKey;

public class MissingHBRowKeyFieldsException extends IllegalArgumentException {
    public MissingHBRowKeyFieldsException(Class clazz) {
        super(String.format("Class %s doesn't even have a single field annotated with %s (how else would you construct the row key for HBase record?)", clazz.getName(), HBRowKey.class.getName()));
    }
}
