package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.CrawlNoVersion;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CrawlNoVersionDAO extends AbstractHBDAO<String, CrawlNoVersion> {

    public CrawlNoVersionDAO(Connection connection) throws IOException {
        super(connection);
    }
}
