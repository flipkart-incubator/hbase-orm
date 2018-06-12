package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;

/**
 * Entities that need to be mapped to HBase tables need to implement this generic interface
 *
 * @param <R> Data type for row key. This type must be '{@link Comparable} with itself' and {@link Serializable} (e.g. {@link String}, {@link Integer} etc. or your own custom class).
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
