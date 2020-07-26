package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBTable;

public class ColumnFamilyNotInHBTableException extends IllegalArgumentException {
    public ColumnFamilyNotInHBTableException(String clazzName, String fieldName, String family, String column) {
        super(String.format("Field '%s' of class '%s' is mapped to HBase column '%s:%s' - but column family '%s' isn't specified in class's @%s annotation",
                fieldName, clazzName, family, column, family, HBTable.class.getSimpleName()
        ));
    }
}
