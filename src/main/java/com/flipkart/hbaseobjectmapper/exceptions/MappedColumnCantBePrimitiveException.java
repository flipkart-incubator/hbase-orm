package com.flipkart.hbaseobjectmapper.exceptions;

public class MappedColumnCantBePrimitiveException extends IllegalArgumentException {
    public MappedColumnCantBePrimitiveException(String s) {
        super(s);
    }

}
