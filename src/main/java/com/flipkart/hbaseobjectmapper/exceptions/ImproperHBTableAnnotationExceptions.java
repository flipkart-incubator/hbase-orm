package com.flipkart.hbaseobjectmapper.exceptions;

public class ImproperHBTableAnnotationExceptions {

    public static class MissingHBTableAnnotationException extends IllegalArgumentException {
        public MissingHBTableAnnotationException(String message) {
            super(message);
        }
    }

    public static class EmptyTableNameOnHBTableAnnotationException extends IllegalArgumentException {
        public EmptyTableNameOnHBTableAnnotationException(String message) {
            super(message);
        }
    }

    public static class EmptyColumnFamilyOnHBTableAnnotationException extends IllegalArgumentException {
        public EmptyColumnFamilyOnHBTableAnnotationException(String message) {
            super(message);
        }
    }

    public static class InvalidValueForVersionsOnHBTableAnnotationException extends IllegalArgumentException {
        public InvalidValueForVersionsOnHBTableAnnotationException(String message) {
            super(message);
        }

    }

    public static class DuplicateColumnFamilyNamesOnHBTableAnnotationException extends IllegalArgumentException {
        public DuplicateColumnFamilyNamesOnHBTableAnnotationException(String message) {
            super(message);
        }
    }
}
