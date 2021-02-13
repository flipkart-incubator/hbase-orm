package com.flipkart.hbaseobjectmapper;

import java.io.Closeable;
import java.io.Serializable;

/**
 * This class is the return type of all 'records' methods of {@link AbstractHBDAO} &amp; {@link ReactiveHBDAO} classes, which enable you to iterate over large number of records
 * (e.g. {@link AbstractHBDAO#records(Serializable, Serializable) AbstractHBDAO.records(R, R)}).
 * <br><br>
 * Users of this library are <u>not</u> expected to instantiate this class on their own.
 * <br><br>
 * <b>Note</b>: This class is <u>not</u> thread-safe. If you intend to scan records across multiple threads, keep different filter criteria for each thread.
 *
 * @param <T> record type
 */
@SuppressWarnings("rawtypes")
public interface Records<T extends HBRecord> extends Closeable, Iterable<T> {
}
