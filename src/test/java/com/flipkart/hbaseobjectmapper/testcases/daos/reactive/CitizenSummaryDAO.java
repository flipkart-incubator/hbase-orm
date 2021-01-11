package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;


import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.CitizenSummary;

import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;

public class CitizenSummaryDAO extends ReactiveHBDAO<String, CitizenSummary> {

    public CitizenSummaryDAO(AsyncConnection connection) {
        super(connection);
    }
}
