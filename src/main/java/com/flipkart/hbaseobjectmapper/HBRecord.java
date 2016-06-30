package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;

/**
 * Entities that need to be mapped to HBase tables need to implement this interface
 *
 * @param <R> A type that's {@link Comparable} to itself and is {@link Serializable} (e.g. <code>String</code>, <code>Integer</code> etc)
 */
public interface HBRecord<R extends Serializable & Comparable<R>> extends Serializable {

    /**
     * Composes the row key required for HBase from class variables
     *
     * @return Row key as {@link Serializable} data type
     */
    R composeRowKey();

    /**
     * Assigns the class variables from row key string (from HBase) provided
     *
     * @param rowKey Row key as {@link Serializable} data type
     */
    void parseRowKey(R rowKey);

}
