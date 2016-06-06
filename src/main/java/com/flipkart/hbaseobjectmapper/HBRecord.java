package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;

/**
 * Entities that need to be mapped to HBase table need to implement this interface
 */
public interface HBRecord extends Serializable {

    /**
     * Forms the row key required for HBase from class variables
     *
     * @return Row key as string
     */
    String composeRowKey();

    /**
     * Assigns the class variables from row key string (from HBase) provided
     *
     * @param rowKey Row key  as a string
     */
    void parseRowKey(String rowKey);

}
