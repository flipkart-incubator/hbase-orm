package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.concurrent.*;

import static com.flipkart.hbaseobjectmapper.testcases.util.cluster.RealHBaseCluster.USE_REAL_HBASE;

public class InMemoryHBaseCluster implements HBaseCluster {

    private final static long CLUSTER_DEFAULT_TIMEOUT = 60;
    public static final String INMEMORY_CLUSTER_START_TIMEOUT = "INMEMORY_CLUSTER_START_TIMEOUT";
    private final HBaseTestingUtility utility;
    private final ExecutorService executorService;
    private Connection connection;
    private final long timeout;

    public InMemoryHBaseCluster() {
        this(CLUSTER_DEFAULT_TIMEOUT);
    }

    public InMemoryHBaseCluster(long timeout) {
        this.utility = new HBaseTestingUtility();
        this.executorService = Executors.newSingleThreadExecutor();
        this.timeout = timeout;
    }

    public Connection start() throws IOException {
        try {
            System.out.println("Starting in-memory HBase test cluster");
            executorService.submit(() -> utility.startMiniCluster(1, 1, false)).get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException(String.format(
                    "Starting an 'in-memory HBase cluster' took much longer than expected time of %d seconds. Aborting.%n" +
                            "You may try the following:%n" +
                            "[1] Increase timeout by setting the '%s' environmental variable to a value higher than %d%n" +
                            "[2] Using a machine with better hardware configuration%n" +
                            "[3] Use a real hbase cluster by setting environmental variable '%s' to 'true'%n" +
                            "[4] Skip test cases (not recommended)", timeout, INMEMORY_CLUSTER_START_TIMEOUT, timeout, USE_REAL_HBASE), e);
        } catch (Exception e) {
            throw new IOException("Error starting an in-memory HBase cluster", e);
        }
        connection = utility.getConnection();
        return connection;
    }

    @Override
    public void end() throws Exception {
        connection.close();
        utility.shutdownMiniCluster();
    }
}
