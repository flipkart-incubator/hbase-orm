package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Iterator;

/**
 * Records derived from the synchronous variant of HBase DAO.
 *
 * @param <T> a record type
 */
@SuppressWarnings("rawtypes")
public class SyncRecords<T extends HBRecord> implements Records<T> {
    private final HBObjectMapper hbObjectMapper;
    private final Class<T> clazz;
    private final Table table;
    private final ResultScanner scanner;

    SyncRecords(Connection connection, HBObjectMapper hbObjectMapper, Class<T> clazz, TableName tableName, Scan scan) throws IOException {
        this.hbObjectMapper = hbObjectMapper;
        this.clazz = clazz;
        this.table = connection.getTable(tableName);
        this.scanner = table.getScanner(scan);
    }

    @Override
    public void close() throws IOException {
        scanner.close();
        table.close();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<T> iterator() {
        return new RecordsIterator<>(hbObjectMapper, clazz, scanner.iterator());
    }

}
