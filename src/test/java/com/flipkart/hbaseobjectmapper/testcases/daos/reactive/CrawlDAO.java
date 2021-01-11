package com.flipkart.hbaseobjectmapper.testcases.daos.reactive;


import com.flipkart.hbaseobjectmapper.ReactiveHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Crawl;

import org.apache.hadoop.hbase.client.AsyncConnection;

public class CrawlDAO extends ReactiveHBDAO<String, Crawl> {

    public CrawlDAO(AsyncConnection connection) {
        super(connection);
    }
}
