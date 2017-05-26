package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class EmployeeDAO extends AbstractHBDAO<Long, Employee> {

    public EmployeeDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
