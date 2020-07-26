package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

import java.time.LocalDateTime;

@MappedSuperClass
public abstract class AbstractRecord implements HBRecord<Long> {

    @HBColumn(family = "a", column = "created_at")
    protected LocalDateTime createdAt;
}
