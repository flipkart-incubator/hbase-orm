package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Entities that need to be mapped to HBase tables need to implement this generic interface
 *
 * @param <R> Data type of row key, which should be '{@link Comparable} with itself' and must be {@link Serializable} (e.g. {@link String}, {@link Integer}, {@link BigDecimal} etc. or your own POJO)
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
