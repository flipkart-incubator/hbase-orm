package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * A minimal administrative API (wrapper over HBase's {@link Admin})
 */
public class HBAdmin {
    private final Connection connection;

    /**
     * Constructs {@link HBAdmin} object
     *
     * @param connection HBase connection
     */
    public HBAdmin(Connection connection) {
        this.connection = connection;
    }

    public void createNamespace(String namespace) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.createNamespace(
                    NamespaceDescriptor.create(namespace).build());
        }
    }

    /**
     * Create table represented by the class
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#createTable(TableDescriptor)
     */
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

    /**
     * Deletes HBase table
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#deleteTable(TableName)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.deleteTable(wrappedHBTable.getName());
        }
    }

    /**
     * Enable a
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#enableTable(TableName)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void enableTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.enableTable(wrappedHBTable.getName());
        }
    }

    /**
     * Disable a table
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#disableTable(TableName)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.disableTable(wrappedHBTable.getName());
        }
    }

    /**
     * Checks whether table exists
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @return <code>true</code> if table exists
     * @throws IOException When HBase call fails
     * @see Admin#tableExists(TableName)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            return admin.tableExists(wrappedHBTable.getName());
        }
    }

}
