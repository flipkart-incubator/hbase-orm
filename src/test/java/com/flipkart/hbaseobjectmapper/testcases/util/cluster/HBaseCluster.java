package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface HBaseCluster {

    Connection start() throws IOException;

    CompletableFuture<AsyncConnection> startAsync();

    void end() throws Exception;
}
