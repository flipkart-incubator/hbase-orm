package com.flipkart.hbaseobjectmapper;

import java.lang.reflect.Field;

/**
 * Wrapper for {@link WrappedHBColumn} class. To be used in test cases only.
 */
public class WrappedHBColumnTC extends WrappedHBColumn {
    public WrappedHBColumnTC(Field field) {
        super(field);
    }
}
