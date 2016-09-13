package com.flipkart.hbaseobjectmapper.util.cluster;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface HBaseCluster {

    Configuration init() throws IOException;

    void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException;

    void end() throws Exception;
}
