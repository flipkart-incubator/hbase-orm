package com.flipkart.hbaseobjectmapper.exceptions;

public class AllHBColumnFieldsNullException extends IllegalArgumentException {
    public AllHBColumnFieldsNullException() {
        super("Cannot accept input object with all it's column-mapped variables null");
    }
}
