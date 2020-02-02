package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class RealHBaseCluster implements HBaseCluster {
    public static final String USE_REAL_HBASE = "USE_REAL_HBASE";
    private Connection connection;

    @Override
    public Connection start() throws IOException {
        System.out.println("Connecting to HBase cluster");
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    @Override
    public void end() throws IOException {
        connection.close();
    }
}
