package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

public class RealHBaseCluster implements HBaseCluster {
    private Admin admin;

    @Override
    public Configuration init() throws IOException {
        System.out.println("Connecting to HBase cluster");
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        return configuration;
    }

    @Override
    public void createTable(String table, Map<String, Integer> columnFamiliesAndVersions) throws IOException {
        TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)) {
            System.out.format("Disabling table '%s': ", tableName);
            admin.disableTable(tableName);
            System.out.println("[DONE]");
            System.out.format("Deleting table '%s': ", tableName);
            admin.deleteTable(tableName);
            System.out.println("[DONE]");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (Map.Entry<String, Integer> e : columnFamiliesAndVersions.entrySet()) {
            tableDescriptor.addFamily(new HColumnDescriptor(e.getKey()).setMaxVersions(e.getValue()));
        }
        System.out.format("Creating table '%s': ", tableName);
        admin.createTable(tableDescriptor);
        System.out.println("[DONE]");
    }

    @Override
    public void end() throws Exception {
        admin.close();
    }
}
