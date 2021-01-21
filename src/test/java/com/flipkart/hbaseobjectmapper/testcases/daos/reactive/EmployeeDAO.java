package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;


import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;

import org.apache.hadoop.hbase.client.AsyncConnection;

public class EmployeeDAO extends ReactiveHBDAO<Long, Employee> {

    public EmployeeDAO(AsyncConnection connection) {
        super(connection);
    }
}
