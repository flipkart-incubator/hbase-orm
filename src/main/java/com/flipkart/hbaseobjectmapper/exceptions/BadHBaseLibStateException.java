package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;

/**
 * Thrown when {@link HBObjectMapper HBObjectMapper} fails to convert bean-like objects to HBase data types or vice-versa
 */
public class BadHBaseLibStateException extends IllegalStateException {
    private static final String BAD_HBASE_STATE_ERROR = "Unknown error - possibly, HBase library is unavailable at runtime or an incorrect/unsupported version of HBase is being used";

    public BadHBaseLibStateException(Throwable throwable) {
        super(BAD_HBASE_STATE_ERROR, throwable);
    }
}
