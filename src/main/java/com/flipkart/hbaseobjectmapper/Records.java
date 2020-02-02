package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.util.Iterator;

/**
 * This class is the return type of all 'records' methods of {@link AbstractHBDAO} class, which enable you to iterate over large number of records (e.g. {@link AbstractHBDAO#records(Serializable, Serializable) AbstractHBDAO.records(R, R)})
 * <br><br>
 * Users of this library are <u>not</u> expected to instantiate this class on their own.
 * <br><br>
 * <b>Note</b>: This class is <u>not</u> thread-safe. If you intend to scan records across multiple threads, keep different filter criteria for each thread.
 *
 * @param <T> record type
 */
@SuppressWarnings("rawtypes")
public class Records<T extends HBRecord> implements Closeable, Iterable<T> {
    private final HBObjectMapper hbObjectMapper;
    private final Class<T> clazz;
    private final Table table;
    private final ResultScanner scanner;

    Records(Connection connection, HBObjectMapper hbObjectMapper, Class<T> clazz, TableName tableName, Scan scan) throws IOException {
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
