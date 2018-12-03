package com.flipkart.hbaseobjectmapper.testcases.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Counter;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CounterDAO extends AbstractHBDAO<String, Counter> {
    public CounterDAO(Connection connection) throws IOException {
        super(connection);
    }
}
