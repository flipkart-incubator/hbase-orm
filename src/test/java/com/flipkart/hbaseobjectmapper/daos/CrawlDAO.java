package com.flipkart.hbaseobjectmapper.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.entities.Crawl;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CrawlDAO extends AbstractHBDAO<String, Crawl> {

    public CrawlDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
