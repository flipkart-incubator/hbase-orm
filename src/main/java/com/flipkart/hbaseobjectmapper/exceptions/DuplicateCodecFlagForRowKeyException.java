package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;

import java.io.Serializable;

public class DuplicateCodecFlagForRowKeyException extends IllegalArgumentException {
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> DuplicateCodecFlagForRowKeyException(Class<T> clazz, String flagName) {
        super(String.format("The %s annotation on %s class has duplicate codec flags. See codec flag '%s'.", HBTable.class.getSimpleName(), clazz.getName(), flagName));
    }
}
