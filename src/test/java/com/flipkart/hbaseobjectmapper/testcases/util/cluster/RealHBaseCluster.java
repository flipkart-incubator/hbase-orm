package com.flipkart.hbaseobjectmapper.testcases.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Map;

public class RealHBaseCluster implements HBaseCluster {
    private HBaseAdmin hBaseAdmin;

    @Override
    public Configuration init() throws IOException {
        System.out.println("Connecting to HBase cluster");
        Configuration configuration = HBaseConfiguration.create();
        hBaseAdmin = new HBaseAdmin(configuration);
        return configuration;
    }

    @Override
    public void createTable(String table, Map<String, Integer> columnFamiliesAndVersions) throws IOException {
        TableName tableName = TableName.valueOf(table);
        if (hBaseAdmin.tableExists(tableName)) {
            System.out.format("Disabling table '%s': ", tableName);
            hBaseAdmin.disableTable(tableName);
            System.out.println("[DONE]");
            System.out.format("Deleting table '%s': ", tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("[DONE]");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (Map.Entry<String, Integer> e : columnFamiliesAndVersions.entrySet()) {
            tableDescriptor.addFamily(new HColumnDescriptor(e.getKey()).setMaxVersions(e.getValue()));
        }
        System.out.format("Creating table '%s': ", tableName);
        hBaseAdmin.createTable(tableDescriptor);
        System.out.println("[DONE]");
    }

    @Override
    public void end() throws Exception {
        hBaseAdmin.close();
    }
}
