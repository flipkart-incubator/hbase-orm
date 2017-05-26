package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {

    public CitizenDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
