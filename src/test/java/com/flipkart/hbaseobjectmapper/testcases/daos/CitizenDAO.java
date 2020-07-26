package com.flipkart.hbaseobjectmapper.testcases.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import org.apache.hadoop.hbase.client.Connection;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {

    public CitizenDAO(Connection connection) {
        super(connection);
    }
}
