package com.flipkart.hbaseobjectmapper.exceptions;

public class InvalidReadVersionsCountException extends IllegalArgumentException {
    public InvalidReadVersionsCountException(final String aCause) {
        super(aCause);
    }
}
