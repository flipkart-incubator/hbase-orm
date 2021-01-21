package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;

import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;

import org.apache.hadoop.hbase.client.AsyncConnection;

public class CitizenDAO extends ReactiveHBDAO<String, Citizen> {

    public CitizenDAO(AsyncConnection connection) {
        super(connection);
    }
}
