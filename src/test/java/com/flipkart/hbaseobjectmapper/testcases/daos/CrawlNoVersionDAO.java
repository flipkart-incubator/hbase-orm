package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.CrawlNoVersion;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CrawlNoVersionDAO extends AbstractHBDAO<String, CrawlNoVersion> {

    public CrawlNoVersionDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
