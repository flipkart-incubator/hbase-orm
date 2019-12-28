package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Crawl;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CrawlDAO extends AbstractHBDAO<String, Crawl> {

    public CrawlDAO(Connection connection) throws IOException {
        super(connection);
    }
}
