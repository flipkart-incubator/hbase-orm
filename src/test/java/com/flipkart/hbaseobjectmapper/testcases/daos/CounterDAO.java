package com.flipkart.hbaseobjectmapper.testcases.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Counter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CounterDAO extends AbstractHBDAO<String, Counter> {
    public CounterDAO(Configuration configuration) throws IOException {
        super(configuration);
    }
}
