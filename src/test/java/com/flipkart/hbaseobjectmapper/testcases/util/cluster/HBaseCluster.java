package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

public interface HBaseCluster {

    Configuration init() throws IOException;

    void createTable(String tableName, Map<String, Integer> columnFamiliesAndVersions) throws IOException;

    void end() throws Exception;
}
