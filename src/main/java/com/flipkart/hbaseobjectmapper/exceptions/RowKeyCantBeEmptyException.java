package com.flipkart.hbaseobjectmapper.exceptions;

public class RowKeyCantBeEmptyException extends IllegalArgumentException {
    public RowKeyCantBeEmptyException() {
        super("Row key composed for object is null or empty");
    }
}
