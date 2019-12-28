package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.client.Result;

import java.util.Iterator;

/**
 * Iterator implementation, for internal use only
 *
 * @param <T> Data type of record
 */
@SuppressWarnings("rawtypes")
class RecordsIterator<T extends HBRecord> implements Iterator<T> {
    private final HBObjectMapper hbObjectMapper;
    private final Class<T> clazz;
    private final Iterator<Result> resultIterator;

    public RecordsIterator(HBObjectMapper hbObjectMapper, Class<T> clazz, Iterator<Result> resultIterator) {
        this.hbObjectMapper = hbObjectMapper;
        this.clazz = clazz;
        this.resultIterator = resultIterator;
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        Result result = resultIterator.next();
        return (T) hbObjectMapper.readValue(result, clazz);
    }

}
