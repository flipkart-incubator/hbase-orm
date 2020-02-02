package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public interface HBaseCluster {

    Connection start() throws IOException;

    void end() throws Exception;
}
