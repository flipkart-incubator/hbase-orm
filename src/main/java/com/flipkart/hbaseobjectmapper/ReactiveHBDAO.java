package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A reactive <i>Data Access Object</i> (DAO) class that enables simple random access (read/write) of HBase rows.
 * This implementation aims to be capability conformant with {@link AbstractHBDAO}.
 *
 * @param <R> Data type of row key, which should be '{@link Comparable} with itself' and must be {@link Serializable} (e.g. {@link String}, {@link Integer}, {@link BigDecimal} etc. or your own POJO)
 * @param <T> Entity type that maps to an HBase row (this type must have implemented {@link HBRecord} interface)
 */
public abstract class ReactiveHBDAO<R extends Serializable & Comparable<R>, T extends HBRecord<R>> extends BaseHBDAO<R, T> {

    protected final AsyncConnection connection;

    /**
     * Constructs a data access object using your custom {@link HBObjectMapper}.
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default {@link HBObjectMapper}, just use the constructor {@link #ReactiveHBDAO(AsyncConnection)} (AsyncConnection)}
     *
     * @param connection     HBase Connection
     * @param hbObjectMapper Your custom {@link HBObjectMapper}
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final AsyncConnection connection, @Nonnull final HBObjectMapper hbObjectMapper) {
        super(hbObjectMapper);
        this.connection = connection;
    }

    /**
     * Constructs a data access object using your custom {@link HBObjectMapper}.
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default {@link HBObjectMapper}, just use the constructor {@link #ReactiveHBDAO(Configuration)}
     *
     * @param configuration  Hadoop configuration
     * @param hbObjectMapper Your custom {@link HBObjectMapper}
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final Configuration configuration, @Nonnull final HBObjectMapper hbObjectMapper) {
        this(ConnectionFactory.createAsyncConnection(configuration).join(), hbObjectMapper);
    }

    /**
     * Constructs a data access object using your custom codec.
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default codec, just use the constructor {@link #ReactiveHBDAO(AsyncConnection)}
     *
     * @param connection HBase Connection
     * @param codec      Your custom codec. If <code>null</code>, default codec is used.
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final AsyncConnection connection, @Nonnull final Codec codec) {
        this(connection, HBObjectMapperFactory.construct(codec));
    }

    /**
     * Constructs a data access object using your custom codec.
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default codec, just use the constructor {@link #ReactiveHBDAO(Configuration)}
     *
     * @param configuration Hadoop configuration
     * @param codec         Your custom codec. If <code>null</code>, default codec is used.
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final Configuration configuration, @Nonnull final Codec codec) {
        this(ConnectionFactory.createAsyncConnection(configuration).join(), HBObjectMapperFactory.construct(codec));
    }

    /**
     * Constructs a data access object.
     *
     * @param connection HBase Connection
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final AsyncConnection connection) {
        this(connection, HBObjectMapperFactory.construct());
    }

    /**
     * Constructs a data access object.
     *
     * @param configuration Hadoop configuration
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected ReactiveHBDAO(@Nonnull final Configuration configuration) {
        this(configuration, HBObjectMapperFactory.construct());
    }

    /**
     * Get specified number of versions of a row from HBase table by it's row key
     *
     * @param rowKey             Row key
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException when the versions specified is invalid
     */
    public CompletableFuture<T> get(@Nonnull final R rowKey, final int numVersionsToFetch) throws IOException {

        final Get get = new Get(toBytes(rowKey)).readVersions(numVersionsToFetch);
        return getHBaseTable()
                .get(get)
                .thenApply(mapResultToRecordType());
    }

    /**
     * Get a row from HBase table by it's row key
     *
     * @param rowKey Row key
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException when HBase Get fails to be constructed, should not occur.
     */
    public CompletableFuture<T> get(@Nonnull final R rowKey) throws IOException {
        return get(rowKey, 1);
    }

    /**
     * Fetch an HBase row for a given {@link Get} object
     *
     * @param get HBase's Get object, typically formed using the {@link #getGet(Serializable) getGet(R)} method
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     */
    public CompletableFuture<T> getOnGet(@Nonnull final Get get) {

        return getHBaseTable()
                .get(get)
                .thenApply(mapResultToRecordType());
    }

    /**
     * @param gets List of {@link Get} objects for which rows have to be fetched
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     */
    @SuppressWarnings("unused")
    public Stream<CompletableFuture<T>> getOnGets(@Nonnull final List<Get> gets) {

        return getHBaseTable()
                .get(gets)
                .stream()
                .map(resultCompletableFuture -> resultCompletableFuture.thenApply(mapResultToRecordType()));
    }

    /**
     * Get specified number of versions of rows from HBase table by array of row keys (This method is a bulk variant of {@link #get(Serializable, int) get(R, int)} method)
     *
     * @param rowKeys            Row keys to fetch
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException when the versions specified is invalid
     */
    public Stream<CompletableFuture<T>> get(@Nonnull final R[] rowKeys, int numVersionsToFetch) throws IOException {

        final List<Get> gets = new ArrayList<>(rowKeys.length);
        for (final R rowKey : rowKeys) {
            gets.add(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
        }

        return getOnGets(gets);
    }

    /**
     * Get records by array of row keys (This method is a bulk variant of {@link #get(Serializable) get(R)} method)
     *
     * @param rowKeys Row keys to fetch
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public Stream<CompletableFuture<T>> get(@Nonnull final R[] rowKeys) throws IOException {
        return get(rowKeys, 1);
    }

    /**
     * Get specified number of versions of rows from HBase table by a range of row keys - start key (inclusive) to end key (exclusive)
     * <br><br>
     * This method is a multi-version variant of {@link #get(Serializable, Serializable) get(R, R)}
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given start and end row keys, do <u>not</u> use this method. Use the iterable variant {@link #records(Serializable, boolean, Serializable, boolean, int, int) records(R, boolean, R, boolean, int, int)} instead.
     *
     * @param startRowKey        Row start
     * @param endRowKey          Row end
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     */
    public CompletableFuture<List<T>> get(@Nonnull final R startRowKey, @Nonnull final R endRowKey, int numVersionsToFetch) {
        return get(startRowKey, true, endRowKey, false, numVersionsToFetch);
    }

    /**
     * Get specified number of versions of rows from HBase table by a range of row keys - start key (inclusive) to end key (exclusive)
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given start and end row keys, do <u>not</u> use this method. Use the iterable variant {@link #records(Serializable, boolean, Serializable, boolean, int, int) records(R, boolean, R, boolean, int, int)} instead.
     *
     * @param startRowKey        Row start
     * @param endRowKey          Row end
     * @param startRowInclusive  whether we should include the start row when scan?
     * @param endRowInclusive    whether we should include the end row when scan?
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     */
    public CompletableFuture<List<T>> get(@Nonnull final R startRowKey, final boolean startRowInclusive, @Nonnull final R endRowKey, final boolean endRowInclusive, final int numVersionsToFetch) {
        final Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey), startRowInclusive)
                .withStopRow(toBytes(endRowKey), endRowInclusive)
                .readVersions(numVersionsToFetch);
        return get(scan);
    }

    /**
     * Get records from HBase table for a given {@link Scan} object.
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given scan criteria, do <u>not</u> use this method. Use the iterable variant {@link #records(Scan)} instead.
     *
     * @param scan HBase's scan object
     * @return Records corresponding to {@link Scan} object passed, deserialized as objects of your bean-like class
     */
    public CompletableFuture<List<T>> get(@Nonnull final Scan scan) {

        return getHBaseTable()
                .scanAll(scan)
                .thenApply(results -> results.stream().map(mapResultToRecordType()).collect(Collectors.toList()));
    }

    /**
     * Get specified number of versions of rows by a range of row keys (start to end)
     *
     * @param startRowKey Row start
     * @param endRowKey   Row end
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     */
    public CompletableFuture<List<T>> get(@Nonnull final R startRowKey, @Nonnull final R endRowKey) {
        return get(startRowKey, endRowKey, 1);
    }

    /**
     * Get records whose row keys match provided prefix
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given row key prefix, do <u>not</u> use this method. Use the iterable variant {@link #recordsByPrefix(byte[], int)} instead.
     *
     * @param rowPrefix          Prefix to scan for
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Records corresponding to provided prefix, deserialized as list of objects of your bean-like class
     */
    public CompletableFuture<List<T>> getByPrefix(@Nonnull final byte[] rowPrefix, int numVersionsToFetch) {
        final Scan scan = new Scan()
                .setRowPrefixFilter(rowPrefix)
                .readVersions(numVersionsToFetch);
        return get(scan);
    }

    /**
     * Get records whose row keys match provided prefix
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given row key prefix, do <u>not</u> use this method. Use the iterable variant {@link #recordsByPrefix(byte[])} instead.
     *
     * @param rowPrefix Prefix to scan for
     * @return Records corresponding to {@link Scan} object passed, deserialized as objects of your bean-like class
     */
    public CompletableFuture<List<T>> getByPrefix(@Nonnull final byte[] rowPrefix) {
        return getByPrefix(rowPrefix, 1);
    }

    /**
     * Get an iterable to iterate over records matching given {@link Scan} object
     *
     * @param scan HBase's scan object
     * @return An iterable to iterate over records matching the scan criteria
     */
    public Records<T> records(@Nonnull final Scan scan) {
        return new ReactiveRecords<>(getHBaseTable().getScanner(scan), hbObjectMapper, hbRecordClass);
    }

    /**
     * Get an iterable to iterate over records matching given row key prefix
     *
     * @param rowPrefix Prefix to scan for
     * @return An iterable to iterate over records matching the scan criteria
     */
    public Records<T> recordsByPrefix(@Nonnull final byte[] rowPrefix) {
        return recordsByPrefix(rowPrefix, 1);
    }

    /**
     * Get an iterable to iterate over records matching given row key prefix and fetch specific number of versions
     *
     * @param rowPrefix          Prefix to scan for
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return An iterable over objects of your bean-like class
     */
    public Records<T> recordsByPrefix(@Nonnull final byte[] rowPrefix, int numVersionsToFetch) {
        final Scan scan = new Scan()
                .setRowPrefixFilter(rowPrefix)
                .readVersions(numVersionsToFetch);
        return records(scan);
    }

    /**
     * Get an iterable to iterate over records matching range of row keys (start to end)
     *
     * @param startRowKey Row start (inclusive)
     * @param endRowKey   Row end (exclusive)
     * @return An iterable over objects of your bean-like class
     */
    public Iterable<T> records(@Nonnull final R startRowKey, @Nonnull final R endRowKey) {
        Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey))
                .withStopRow(toBytes(endRowKey));
        return records(scan);
    }

    /**
     * Get an iterable to iterate over records matching range of row keys (start to end) and other criteria
     * <br><br>
     * <b>Note:</b> If you need advanced scanning, consider using {@link #records(Scan)}
     *
     * @param startRowKey        Row start
     * @param startRowInclusive  whether we should include the start row when scan
     * @param endRowKey          Row end
     * @param endRowInclusive    whether we should include the end row when scan
     * @param numVersionsToFetch Number of versions to be retrieved
     * @param numRowsForCaching  Number of rows for caching (higher values are faster but take more memory)
     * @return An iterable over objects of your bean-like class
     * @see <a href="https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html#setCaching-int-">HBase Scan caching</a>
     */
    public Records<T> records(@Nonnull final R startRowKey, final boolean startRowInclusive, @Nonnull final R endRowKey, final boolean endRowInclusive, final int numVersionsToFetch, final int numRowsForCaching) {
        final Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey), startRowInclusive)
                .withStopRow(toBytes(endRowKey), endRowInclusive)
                .readVersions(numVersionsToFetch)
                .setCaching(numRowsForCaching);
        return records(scan);
    }

    /**
     * Get an iterable to iterate over records matching range of row keys (start to end) and other criteria
     *
     * @param startRowKey        Row start
     * @param endRowKey          Row end
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return An iterable over objects of your bean-like class
     */
    public Records<T> records(@Nonnull final R startRowKey, @Nonnull final R endRowKey, final int numVersionsToFetch) {
        final Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey))
                .withStopRow(toBytes(endRowKey))
                .readVersions(numVersionsToFetch);
        return records(scan);
    }

    /**
     * Increments field by specified amount
     *
     * @param rowKey    Row key of the record whose column needs to be incremented
     * @param fieldName Field that needs to be incremented (this must be of {@link Long} type)
     * @param amount    Amount by which the HBase column needs to be incremented
     * @return The new value, post increment
     */
    public CompletableFuture<Long> increment(@Nonnull final R rowKey, @Nonnull final String fieldName, final long amount) {
        final WrappedHBColumn hbColumn = validateAndGetLongColumn(fieldName);

        return getHBaseTable()
                .incrementColumnValue(toBytes(rowKey), hbColumn.familyBytes(), hbColumn.columnBytes(), amount);
    }

    /**
     * Increments field by specified amount
     *
     * @param rowKey     Row key of the record whose column needs to be incremented
     * @param fieldName  Field that needs to be incremented (this must be of {@link Long} type)
     * @param amount     Amount by which the HBase column needs to be incremented
     * @param durability The persistence guarantee for this increment (see {@link Durability})
     * @return The new value, post increment
     */
    public CompletableFuture<Long> increment(@Nonnull final R rowKey, @Nonnull final String fieldName, final long amount, @Nonnull final Durability durability) {
        final WrappedHBColumn hbColumn = validateAndGetLongColumn(fieldName);
        return getHBaseTable()
                .incrementColumnValue(toBytes(rowKey), hbColumn.familyBytes(), hbColumn.columnBytes(), amount, durability);
    }

    /**
     * Performs HBase {@link Table#increment} on the given {@link Increment} object <br>
     * <br>
     * <b>Note</b>: <ul>
     * <li>You may construct {@link Increment} object using the {@link #getIncrement(Serializable) getIncrement(R)} method</li>
     * <li>Unlike the {@link #increment(Serializable, String, long)} methods, this method skips some validations (hence, be cautious)</li>
     * </ul>
     *
     * @param increment HBase Increment object
     * @return <b>Partial object</b> containing (only) values  of fields that were incremented
     */
    public CompletableFuture<T> increment(@Nonnull final Increment increment) {

        return getHBaseTable()
                .increment(increment)
                .thenApply(mapResultToRecordType());
    }

    /**
     * Appends value of a field with the value provided. If the field is empty, the value provided becomes the value of the field.
     * <br><br>
     * This operation applies to fields of types such as {@link String}. Use this on non-{@link String} fields only if you understand the semantics.
     *
     * @param rowKey        Row key of the record
     * @param fieldName     Name of the field whose value needs to be appended
     * @param valueToAppend Value to be appended
     * @return <b>Partial object</b> containing (only) value of field that was appended
     * @see Table#append(Append)
     * @see #append(Serializable, Map) append(R, Map)
     */
    public CompletableFuture<T> append(@Nonnull final R rowKey, @Nonnull final String fieldName, @Nonnull final Object valueToAppend) {
        final Map<String, Object> one = new HashMap<>(1);
        one.put(fieldName, valueToAppend);
        return append(rowKey, one);
    }

    /**
     * Appends values of fields with the values provided. For empty fields, the value provided becomes the value of the field.
     * <br><br>
     * This operation applies to fields of types such as {@link String}. Use this on non-{@link String} fields only if you understand the semantics.
     *
     * @param rowKey         Row key of the record
     * @param valuesToAppend Map of field name of value to be appended
     * @return <b>Partial object</b> containing (only) values of fields that were appended
     * @see Table#append(Append)
     * @see #append(Serializable, String, Object) append(R, String, Object)
     */
    public CompletableFuture<T> append(@Nonnull final R rowKey, @Nonnull final Map<String, Object> valuesToAppend) {
        final Append append = getAppend(rowKey);
        for (final Map.Entry<String, Object> e : valuesToAppend.entrySet()) {
            final String fieldName = e.getKey();
            final Field field = getField(fieldName);
            final Object value = e.getValue();
            if (!field.getType().isAssignableFrom(value.getClass())) {
                throw new IllegalArgumentException(String.format("An attempt was made to append a value of type '%s' to field '%s', which is of type '%s' (incompatible)", value.getClass(), fieldName, field.getType()));
            }
            final WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            append.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes(),
                    hbObjectMapper.valueToByteArray((Serializable) value, hbColumn.codecFlags())
            );
        }
        return append(append);
    }

    /**
     * Performs HBase's {@link Table#append} on the given {@link Append} object <br>
     * <br>
     * <b>Note</b>: <ul>
     * <li>You may construct {@link Append} object using the {@link #getAppend(Serializable) getAppend(R)} method</li>
     * <li>Unlike the {@link #append(Serializable, String, Object) append(R, String, Object)} and related methods, this method skips some validations. So, use this only if you need access to HBase's native methods.</li>
     * </ul>
     *
     * @param append HBase's {@link Append} object
     * @return <b>Partial object</b> containing (only) values  of fields that were appended
     */
    public CompletableFuture<T> append(@Nonnull final Append append) {

        return getHBaseTable()
                .append(append)
                .thenApply(mapResultToRecordType());
    }

    /**
     * Persist your bean-like object (of a class that implements {@link HBRecord}) to HBase table
     *
     * @param record Object that needs to be persisted
     * @return Row key of the persisted object, represented as a {@link String}
     */
    public CompletableFuture<R> persist(@Nonnull final T record) {

        final Put put = hbObjectMapper.writeValueAsPut0(record);
        return getHBaseTable()
                .put(put)
                .thenApply(nothing -> record.composeRowKey());
    }

    /**
     * Persist a list of your bean-like objects (of a class that implements {@link HBRecord}) to HBase table (this is a bulk variant of {@link #persist(HBRecord)} method)
     *
     * @param records List of objects that needs to be persisted
     * @return Row keys of the persisted objects, represented as a {@link String}
     */
    public Stream<CompletableFuture<R>> persist(@Nonnull final List<T> records) {
        final List<Put> puts = new ArrayList<>(records.size());
        final List<R> rowKeys = new ArrayList<>(records.size());
        for (final T record : records) {
            puts.add(hbObjectMapper.writeValueAsPut0(record));
            rowKeys.add(record.composeRowKey());
        }

        final List<CompletableFuture<Void>> putResults = getHBaseTable()
                .put(puts);
        return IntStream
                .range(0, putResults.size())
                .mapToObj(index -> putResults.get(index).thenApply(nothing -> rowKeys.get(index)));
    }

    /**
     * Delete a row from an HBase table for a given row key
     *
     * @param rowKey row key to delete
     * @return nothing or an error if the operation has failed
     */
    public CompletableFuture<Void> delete(@Nonnull final R rowKey) {
        final Delete delete = new Delete(toBytes(rowKey));

        return getHBaseTable().delete(delete);
    }

    /**
     * Delete HBase row by object (of class that implements {@link HBRecord}
     *
     * @param record Object to delete
     * @return nothing or an error if the operation has failed
     */
    public CompletableFuture<Void> delete(@Nonnull final T record) {
        return this.delete(record.composeRowKey());
    }

    /**
     * Delete HBase rows for an array of row keys
     *
     * @param rowKeys row keys to delete
     * @return a stream of void results or error if the corresponding operation has failed
     */
    public Stream<CompletableFuture<Void>> delete(@Nonnull final R[] rowKeys) {
        final List<Delete> deletes = new ArrayList<>(rowKeys.length);
        for (final R rowKey : rowKeys) {
            deletes.add(new Delete(toBytes(rowKey)));
        }

        return getHBaseTable().delete(deletes)
                .stream();
    }

    /**
     * Delete HBase rows by object references
     *
     * @param records Records to delete
     * @return a stream of void results or error if the corresponding operation has failed
     */
    public Stream<CompletableFuture<Void>> delete(@Nonnull final List<T> records) {
        final List<Delete> deletes = new ArrayList<>(records.size());
        for (final T record : records) {
            deletes.add(new Delete(toBytes(record.composeRowKey())));
        }

        return getHBaseTable().delete(deletes)
                .stream();
    }

    /**
     * Fetch value of column for a given row key and field
     *
     * @param rowKey    Row key to reference HBase row
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Value of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     */
    public CompletableFuture<Object> fetchFieldValue(@Nonnull final R rowKey, @Nonnull final String fieldName) {

        return fetchFieldValue(rowKey, fieldName, 1)
                .thenApply(fieldValues -> {
                    if (fieldValues == null || fieldValues.isEmpty()) {
                        return null;
                    } else {
                        return fieldValues.lastEntry().getValue();
                    }
                });
    }

    /**
     * Fetch multiple versions of column values by row key and field name
     *
     * @param rowKey             Row key to reference HBase row
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return {@link NavigableMap} of timestamps and values of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     */
    public CompletableFuture<NavigableMap<Long, Object>> fetchFieldValue(@Nonnull final R rowKey, @Nonnull final String fieldName, int numVersionsToFetch) {
        @SuppressWarnings("unchecked") R[] array = (R[]) Array.newInstance(rowKeyClass, 1);
        array[0] = rowKey;
        return fetchFieldValues(array, fieldName, numVersionsToFetch)
                .thenApply(map -> map.get(rowKey));
    }

    /**
     * Fetch values of an HBase column for a range of row keys (start and end) and field name
     *
     * @param startRowKey Start row key (scan start)
     * @param endRowKey   End row key (scan end)
     * @param fieldName   Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Map of row key and column value
     */
    public CompletableFuture<Map<R, Object>> fetchFieldValues(@Nonnull final R startRowKey, @Nonnull final R endRowKey, @Nonnull final String fieldName) {

        return fetchFieldValues(startRowKey, endRowKey, fieldName, 1)
                .thenApply(multiVersionedMap -> toSingleVersioned(multiVersionedMap, 10));
    }

    /**
     * Fetch column values for a given array of row keys (bulk variant of method {@link #fetchFieldValue(Serializable, String) fetchFieldValue(R, String)})
     *
     * @param rowKeys   Array of row keys to fetch
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Map of row key and column values
     */
    public CompletableFuture<Map<R, Object>> fetchFieldValues(R[] rowKeys, String fieldName) {

        return fetchFieldValues(rowKeys, fieldName, 1)
                .thenApply(multiVersionedMap -> toSingleVersioned(multiVersionedMap, rowKeys.length));
    }

    /**
     * Fetch specified number of versions of values of an HBase column for a range of row keys (start and end) and field name
     *
     * @param startRowKey        Start row key (scan start)
     * @param endRowKey          End row key (scan end)
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Map of row key and column values (versioned)
     */
    public CompletableFuture<NavigableMap<R, NavigableMap<Long, Object>>> fetchFieldValues(@Nonnull final R startRowKey, @Nonnull final R endRowKey, @Nonnull final String fieldName, int numVersionsToFetch) {
        final Field field = getField(fieldName);
        final WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        final Scan scan = new Scan().withStartRow(toBytes(startRowKey)).withStopRow(toBytes(endRowKey));
        scan.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes());
        scan.readVersions(numVersionsToFetch);
        final NavigableMap<R, NavigableMap<Long, Object>> map = new TreeMap<>();

        final ResultScanner resultScanner = getHBaseTable().getScanner(scan);
        for (final Result result : resultScanner) {
            populateFieldValuesToMap(field, result, map);
        }
        return CompletableFuture.completedFuture(map);
    }

    /**
     * Fetch specified number of versions of values of an HBase column for an array of row keys
     *
     * @param rowKeys            Array of row keys to fetch
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Map of row key and column values (versioned)
     */
    public CompletableFuture<Map<R, NavigableMap<Long, Object>>> fetchFieldValues(@Nonnull final R[] rowKeys, @Nonnull final String fieldName, final int numVersionsToFetch) {
        final Field field = getField(fieldName);
        final WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        final List<Get> gets = new ArrayList<>(rowKeys.length);
        for (final R rowKey : rowKeys) {
            final Get get = new Get(toBytes(rowKey));
            try {
                get.readVersions(numVersionsToFetch);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            get.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes());
            gets.add(get);
        }
        final Map<R, NavigableMap<Long, Object>> map = new LinkedHashMap<>(rowKeys.length, 1.0f);

        final List<CompletableFuture<Result>> completableFutures = getHBaseTable()
                .get(gets);

        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                .thenApply(nothing -> {
                    completableFutures.forEach(resultCompletableFuture -> {
                        final Result result = resultCompletableFuture.join();
                        populateFieldValuesToMap(field, result, map);
                    });
                    return map;
                });
    }

    /**
     * Check whether a row exists or not
     *
     * @param rowKey Row key
     * @return <code>true</code> if row with given row key exists
     */
    public CompletableFuture<Boolean> exists(@Nonnull final R rowKey) {

        return getHBaseTable()
                .exists(new Get(toBytes(rowKey)));
    }

    /**
     * Check whether specified rows exist or not
     *
     * @param rowKeys Row keys
     * @return Stream of completable futures with <code>true</code>/<code>false</code> values corresponding to whether row with given row keys exist
     */
    public Stream<CompletableFuture<Boolean>> exists(R[] rowKeys) {
        List<Get> gets = new ArrayList<>(rowKeys.length);
        for (R rowKey : rowKeys) {   
            gets.add(new Get(
                    toBytes(rowKey)
            ));
        }
        return getHBaseTable()
                .exists(gets)
                .stream();
    }

    /**
     * Get reference to HBase table.
     *
     * @return {@link AsyncTable} object
     */
    public AsyncTable<AdvancedScanResultConsumer> getHBaseTable() {
        return connection.getTable(hbTable.getName());
    }
}
