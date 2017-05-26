package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryHBaseCluster implements HBaseCluster {

    private final static long CLUSTER_START_TIMEOUT = 60;
    private final HBaseTestingUtility utility;
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
    public void createTable(String tableName, Map<String, Integer> columnFamiliesAndVersions) throws IOException {
        byte[][] families = new byte[columnFamiliesAndVersions.size()][];
        int[] versions = new int[columnFamiliesAndVersions.size()];
        int i = 0;
        for (Map.Entry<String, Integer> e : columnFamiliesAndVersions.entrySet()) {
            families[i] = Bytes.toBytes(e.getKey());
            versions[i] = e.getValue();
            i++;
        }
        utility.createTable(Bytes.toBytes(tableName), families, versions);
    }

    @Override
    public void end() throws Exception {
        utility.shutdownMiniCluster();
    }
}
