package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class HBAdmin {
    private final Connection connection;

    public HBAdmin(Connection connection) {
        this.connection = connection;
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(wrappedHBTable.getName());
            for (Map.Entry<String, Integer> e : wrappedHBTable.getFamiliesAndVersions().entrySet()) {
                tableDescriptorBuilder.setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(
                                Bytes.toBytes(e.getKey())
                        ).setMaxVersions(e.getValue()).build());
            }
            admin.createTable(tableDescriptorBuilder.build());
        }
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.deleteTable(wrappedHBTable.getName());
        }
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.disableTable(wrappedHBTable.getName());
        }
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            return admin.tableExists(wrappedHBTable.getName());
        }
    }

}
