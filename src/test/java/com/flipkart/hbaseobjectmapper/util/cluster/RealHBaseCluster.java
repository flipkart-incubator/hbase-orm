package com.flipkart.hbaseobjectmapper.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

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
    public void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException {
        if (hBaseAdmin.tableExists(tableName)) {
            System.out.format("Disabling table '%s': ", tableName);
            hBaseAdmin.disableTable(tableName);
            System.out.println("[DONE]");
            System.out.format("Deleting table '%s': ", tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("[DONE]");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily).setMaxVersions(numVersions));
        }
        System.out.format("Creating table '%s': ", tableName);
        hBaseAdmin.createTable(tableDescriptor);
        System.out.println("[DONE]");
    }

    @Override
    public void end() throws Exception {
        // nothing
    }
}
