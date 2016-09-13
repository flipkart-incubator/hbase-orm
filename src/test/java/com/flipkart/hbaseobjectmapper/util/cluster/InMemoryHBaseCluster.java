package com.flipkart.hbaseobjectmapper.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import java.io.IOException;
import java.util.concurrent.*;

public class InMemoryHBaseCluster implements HBaseCluster {

    private final static long CLUSTER_START_TIMEOUT = 60;
    private HBaseTestingUtility utility;
    private final ExecutorService executorService;


    public InMemoryHBaseCluster() {
        this.utility = new HBaseTestingUtility();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public Configuration init() throws IOException {
        try {
            System.out.println("Starting in-memory HBase test cluster");
            executorService.submit(new Callable<MiniHBaseCluster>() {
                @Override
                public MiniHBaseCluster call() throws Exception {
                    return utility.startMiniCluster();
                }
            }).get(CLUSTER_START_TIMEOUT, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Starting an in-memory HBase cluster took much longer than expected (" + CLUSTER_START_TIMEOUT + " seconds). Aborting...", e);
        } catch (Exception e) {
            throw new IOException("Error starting and in-memory HBase cluster", e);
        }
        return utility.getConfiguration();
    }


    @Override
    public void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException {
        byte[][] columnFamiliesBytes = new byte[columnFamilies.length][];
        for (int i = 0; i < columnFamilies.length; i++) {
            columnFamiliesBytes[i] = columnFamilies[i].getBytes();
        }
        utility.createTable(tableName.getBytes(), columnFamiliesBytes, numVersions);
    }

    @Override
    public void end() throws Exception {
        utility.shutdownMiniCluster();
    }
}
