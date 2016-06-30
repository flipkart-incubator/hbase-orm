package com.flipkart.hbaseobjectmapper.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.entities.CrawlNoVersion;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CrawlNoVersionDAO extends AbstractHBDAO<String, CrawlNoVersion> {

    public CrawlNoVersionDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
