package com.flipkart.hbaseobjectmapper.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.entities.CitizenSummary;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CitizenSummaryDAO extends AbstractHBDAO<String, CitizenSummary> {

    public CitizenSummaryDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
