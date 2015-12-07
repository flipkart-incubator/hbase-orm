package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBRecord;

public class FieldNotMappedToHBaseColumnException extends IllegalArgumentException {
    public FieldNotMappedToHBaseColumnException(Class<? extends HBRecord> hbRecordClass, String fieldName) {
        super(String.format("Field %s.%s is not mapped to an HBase column (consider adding %s or %s annotation)", hbRecordClass.getSimpleName(), fieldName, HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()));
    }
}
