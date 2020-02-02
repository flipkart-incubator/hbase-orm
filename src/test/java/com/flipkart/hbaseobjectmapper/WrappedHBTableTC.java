package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;

/**
 * Wrapper for {@link WrappedHBTable} class. To be used in test cases only.
 */
public class WrappedHBTableTC<R extends Serializable & Comparable<R>, T extends HBRecord<R>> extends WrappedHBTable<R, T> {
    public WrappedHBTableTC(Class<T> clazz) {
        super(clazz);
    }
}
