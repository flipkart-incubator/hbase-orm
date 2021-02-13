package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class RealHBaseCluster implements HBaseCluster {
    public static final String USE_REAL_HBASE = "USE_REAL_HBASE";
    private final AtomicReference<Object> connectionRef = new AtomicReference<>();

    @Override
    public Connection start() throws IOException {
        final Configuration configuration = getConfiguration();
        final Connection connection = ConnectionFactory.createConnection(configuration);
        connectionRef.set(connection);
        return connection;
    }

    @Override
    public CompletableFuture<AsyncConnection> startAsync() {
        final Configuration configuration = getConfiguration();
        return ConnectionFactory.createAsyncConnection(configuration)
                .thenApply(conn -> {
                    connectionRef.set(conn);
                    return conn;
                });
    }

    @Override
    public void end() throws IOException {
        if (connectionRef.get() != null) {
            if (connectionRef.get() instanceof Connection) {
                ((Connection) connectionRef.get()).close();
            } else {
                ((AsyncConnection) connectionRef.get()).close();
            }
        }
    }

    private Configuration getConfiguration() {
        System.out.println("Connecting to HBase cluster");
        return HBaseConfiguration.create();
    }
}
