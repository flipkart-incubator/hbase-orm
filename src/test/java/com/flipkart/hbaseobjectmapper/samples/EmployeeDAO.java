package com.flipkart.hbaseobjectmapper.samples;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class EmployeeDAO extends AbstractHBDAO<Employee> {
    protected EmployeeDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
