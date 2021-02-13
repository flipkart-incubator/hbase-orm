package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.hbaseobjectmapper.testcases.util.cluster.RealHBaseCluster.USE_REAL_HBASE;

public class InMemoryHBaseCluster implements HBaseCluster {

    private final static long CLUSTER_DEFAULT_TIMEOUT = 60;
    public static final String INMEMORY_CLUSTER_START_TIMEOUT = "INMEMORY_CLUSTER_START_TIMEOUT";
    private final HBaseTestingUtility utility;
    private final ExecutorService executorService;
    private final AtomicReference<Object> connectionRef;
    private final long timeout;

    public InMemoryHBaseCluster() {
        this(CLUSTER_DEFAULT_TIMEOUT);
    }

    public InMemoryHBaseCluster(long timeout) {
        System.clearProperty("hadoop.tmp.dir");
        System.setProperty("hbase.testing.preserve.testdir", "false");
        this.utility = new HBaseTestingUtility();
        this.executorService = Executors.newSingleThreadExecutor();
        this.timeout = timeout;
        this.connectionRef = new AtomicReference<>();
    }

    public Connection start() throws IOException {
        doStart();
        final Connection connection = utility.getConnection();
        if (connection == null) {
            throw new IllegalStateException("Connection could not be established with in-memory HBase cluster");
        }
        this.connectionRef.set(connection);
        return connection;
    }

    @Override
    public CompletableFuture<AsyncConnection> startAsync() {

        try {
            doStart();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ConnectionFactory.createAsyncConnection(utility.getConfiguration())
                .thenApply(conn -> {
                    connectionRef.set(conn);
                    return conn;
                });
    }

    @Override
    public void end() throws Exception {
        if (connectionRef.get() != null) {
            if (connectionRef.get() instanceof Connection) {
                ((Connection) connectionRef.get()).close();
            } else {
                ((AsyncConnection) connectionRef.get()).close();
            }
        }
        utility.shutdownMiniCluster();
    }

    private void doStart() throws IOException {
        try {
            System.out.println("Starting in-memory HBase test cluster");
            executorService.submit(() -> utility.startMiniCluster(StartMiniClusterOption.builder()
                    .numMasters(1)
                    .numRegionServers(1)
                    .createRootDir(false)
                    .build())).get(timeout, TimeUnit.SECONDS);
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
    }
}
