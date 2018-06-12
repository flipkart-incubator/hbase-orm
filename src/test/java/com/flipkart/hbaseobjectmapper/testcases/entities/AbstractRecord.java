package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.MappedSuperClass;

@MappedSuperClass
public class AbstractRecord {

    @HBColumn(family = "a", column = "created_at")
    protected Long createdAt;
}
