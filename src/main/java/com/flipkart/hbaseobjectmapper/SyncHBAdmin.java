package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * A minimal administrative API (wrapper over HBase's {@link Admin})
 */
class SyncHBAdmin implements HBAdmin {
    private final Connection connection;

    /**
     * Constructs {@link SyncHBAdmin} object
     *
     * @param connection HBase connection
     */
    public SyncHBAdmin(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void createNamespace(String namespace) throws IOException {
        final NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        try (Admin admin = connection.getAdmin()) {
            admin.createNamespace(namespaceDescriptor);
        }
    }

    @Override
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

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.deleteTable(wrappedHBTable.getName());
        }
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void enableTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.enableTable(wrappedHBTable.getName());
        }
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            admin.disableTable(wrappedHBTable.getName());
        }
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(Class<T> hbRecordClass) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
            return admin.tableExists(wrappedHBTable.getName());
        }
    }

}
