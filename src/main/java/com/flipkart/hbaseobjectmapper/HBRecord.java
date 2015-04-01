package com.flipkart.hbaseobjectmapper;

public interface HBRecord {

    /**
     * Forms the row key required for HBase from class variables
     *
     * @return Row key as string
     */
    public String composeRowKey();

    /**
     * Assigns the class variables from row key string (from HBase) provided
     *
     * @param rowKey Row key  as a string
     */
    public void parseRowKey(String rowKey);

}
