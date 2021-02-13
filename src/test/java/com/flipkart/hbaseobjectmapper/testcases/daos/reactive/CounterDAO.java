package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;

import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Counter;

import org.apache.hadoop.hbase.client.AsyncConnection;

public class CounterDAO extends ReactiveHBDAO<String, Counter> {
    public CounterDAO(AsyncConnection connection) {
        super(connection);
    }
}
