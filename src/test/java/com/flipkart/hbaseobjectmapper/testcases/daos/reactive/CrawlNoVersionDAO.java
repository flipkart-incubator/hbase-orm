package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;


import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.CrawlNoVersion;

import org.apache.hadoop.hbase.client.AsyncConnection;

public class CrawlNoVersionDAO extends ReactiveHBDAO<String, CrawlNoVersion> {

    public CrawlNoVersionDAO(AsyncConnection connection) {
        super(connection);
    }
}
