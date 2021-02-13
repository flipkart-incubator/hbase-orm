package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.client.ResultScanner;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * Records derived from the asynchronous variant of HBase DAO.
 *
 * @param <T> a record type
 */
@SuppressWarnings("rawtypes")
public class ReactiveRecords<T extends HBRecord> implements Records<T> {

    private final HBObjectMapper hbObjectMapper;
    private final Class<T> clazz;
    private final ResultScanner scanner;

    public ReactiveRecords(@Nonnull final ResultScanner scanner, @Nonnull final HBObjectMapper hbObjectMapper, @Nonnull final Class<T> clazz) {
        this.hbObjectMapper = hbObjectMapper;
        this.clazz = clazz;
        this.scanner = scanner;
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override @Nonnull
    public Iterator<T> iterator() {
        return new RecordsIterator<>(hbObjectMapper, clazz, scanner.iterator());
    }
}
