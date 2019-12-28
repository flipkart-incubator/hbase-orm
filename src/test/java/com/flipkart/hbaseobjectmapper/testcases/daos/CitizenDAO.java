package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {

    public CitizenDAO(Connection connection) throws IOException {
        super(connection);
    }
}
