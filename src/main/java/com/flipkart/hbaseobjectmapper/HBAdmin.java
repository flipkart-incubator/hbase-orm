package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;

public interface HBAdmin {
    /**
     * Creates a namespace.
     *
     * @param namespace a namespace name
     * @throws IOException on hbase error
     */
    void createNamespace(String namespace) throws IOException;

    /**
     * Create table represented by the class
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#createTable(TableDescriptor)
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTable(Class<T> hbRecordClass) throws IOException;

    /**
     * Deletes HBase table
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#deleteTable(TableName)
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(Class<T> hbRecordClass) throws IOException;

    /**
     * Enable a
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#enableTable(TableName)
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void enableTable(Class<T> hbRecordClass) throws IOException;

    /**
     * Disable a table
     *
     * @param hbRecordClass Class that represents the HBase table
     * @param <R>           Data type of row key
     * @param <T>           Entity type
     * @throws IOException When HBase call fails
     * @see Admin#disableTable(TableName)
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(Class<T> hbRecordClass) throws IOException;

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
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(Class<T> hbRecordClass) throws IOException;

    /**
     * Gets a HBAdmin instance given a connection.
     *
     * @param aConnection a connection instance
     * @return a HBAdmin instance
     * @throws UnsupportedOperationException if the connection class is unrecognized
     */
    static HBAdmin create(@Nonnull Object aConnection) {
        if (aConnection instanceof Connection) {
            return new SyncHBAdmin((Connection) aConnection);
        } else if (aConnection instanceof AsyncConnection) {
            return new AsyncHBAdmin((AsyncConnection) aConnection);
        } else {
            throw new UnsupportedOperationException("Unknown connection type: " + aConnection.getClass());
        }
    }
}
