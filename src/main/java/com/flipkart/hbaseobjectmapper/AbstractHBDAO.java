package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

/**
 * A <i>Data Access Object</i> (DAO) class that enables simple random access (read/write) of HBase rows.
 * <br><br>
 * <b>This class is thread-safe.</b> This is designed such that only one instance of each DAO class needs to be maintained for the entire lifecycle of your program.
 *
 * @param <R> Data type of row key (must be '{@link Comparable} with itself' and must be {@link Serializable})
 * @param <T> Entity type that maps to an HBase row (this type must have implemented {@link HBRecord} interface)
 * @see <a href="https://en.wikipedia.org/wiki/Data_access_object">Data access object</a>
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractHBDAO<R extends Serializable & Comparable<R>, T extends HBRecord<R>> {

    protected final HBObjectMapper hbObjectMapper;
    protected final Connection connection;
    protected final Class<R> rowKeyClass;
    protected final Class<T> hbRecordClass;
    protected final WrappedHBTable<R, T> hbTable;
    private final Map<String, Field> fields;

    /**
     * Constructs a data access object using your custom {@link HBObjectMapper}
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default {@link HBObjectMapper}, just use the constructor {@link #AbstractHBDAO(Connection)}
     *
     * @param connection     HBase Connection
     * @param hbObjectMapper Your custom {@link HBObjectMapper}
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    @SuppressWarnings("unchecked")
    protected AbstractHBDAO(Connection connection, HBObjectMapper hbObjectMapper) {
        this.connection = connection;
        this.hbObjectMapper = hbObjectMapper;
        hbRecordClass = (Class<T>) new TypeToken<T>(getClass()) {
        }.getRawType();
        if (hbRecordClass == null) {
            throw new IllegalStateException("Unable to resolve HBase record type");
        }
        this.hbObjectMapper.validateHBClass(hbRecordClass);
        rowKeyClass = (Class<R>) new TypeToken<R>(getClass()) {
        }.getRawType();
        if (rowKeyClass == null) {
            throw new IllegalStateException("Unable to resolve HBase rowkey type");
        }
        hbTable = new WrappedHBTable<>(hbRecordClass);
        fields = hbObjectMapper.getHBColumnFields0(hbRecordClass);
    }

    /**
     * Constructs a data access object using your custom {@link HBObjectMapper}
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default {@link HBObjectMapper}, just use the constructor {@link #AbstractHBDAO(Configuration)}
     *
     * @param configuration  Hadoop configuration
     * @param hbObjectMapper Your custom {@link HBObjectMapper}
     * @throws IOException           Exceptions thrown by HBase
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected AbstractHBDAO(Configuration configuration, HBObjectMapper hbObjectMapper) throws IOException {
        this(ConnectionFactory.createConnection(configuration), hbObjectMapper);
    }

    /**
     * Constructs a data access object using your custom codec
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default codec, just use the constructor {@link #AbstractHBDAO(Connection)}
     *
     * @param connection HBase Connection
     * @param codec      Your custom codec. If <code>null</code>, default codec is used.
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected AbstractHBDAO(Connection connection, Codec codec) {
        this(connection, HBObjectMapperFactory.construct(codec));
    }

    /**
     * Constructs a data access object using your custom codec
     * <p>
     * <br>
     * <b>Note: </b>If you want to use the default codec, just use the constructor {@link #AbstractHBDAO(Configuration)}
     *
     * @param configuration Hadoop configuration
     * @param codec         Your custom codec. If <code>null</code>, default codec is used.
     * @throws IOException           Exceptions thrown by HBase
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected AbstractHBDAO(Configuration configuration, Codec codec) throws IOException {
        this(ConnectionFactory.createConnection(configuration), HBObjectMapperFactory.construct(codec));
    }

    /**
     * Constructs a data access object
     *
     * @param connection HBase Connection
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected AbstractHBDAO(Connection connection) {
        this(connection, (Codec) null);
    }

    /**
     * Constructs a data access object
     *
     * @param configuration Hadoop configuration
     * @throws IOException           Exceptions thrown by HBase
     * @throws IllegalStateException Annotation(s) on base entity may be incorrect
     */
    protected AbstractHBDAO(Configuration configuration) throws IOException {
        this(configuration, (Codec) null);
    }

    /**
     * Get specified number of versions of a row from HBase table by it's row key
     *
     * @param rowKey             Row key
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(R rowKey, int numVersionsToFetch) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.get(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
            return hbObjectMapper.readValue(rowKey, result, hbRecordClass);
        }
    }

    /**
     * Get a row from HBase table by it's row key
     *
     * @param rowKey Row key
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(R rowKey) throws IOException {
        return get(rowKey, 1);
    }

    /**
     * Creates an HBase {@link Get} object, for enabling specialised read of HBase rows.
     * <br><br>
     * Typically, this is used in {@link #getOnGet(Get)} method
     *
     * @param rowKey Row key
     * @return HBase's Get object
     * @see #getOnGet(Get)
     */
    public Get getGet(R rowKey) {
        return new Get(toBytes(rowKey));
    }

    /**
     * Fetch an HBase row for a given {@link Get} object
     *
     * @param get HBase's Get object, typically formed using the {@link #getGet(Serializable) getGet} method
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T getOnGet(Get get) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.get(get);
            return hbObjectMapper.readValue(result, hbRecordClass);
        }
    }

    /**
     * @param gets List of {@link Get} objects for which rows have to be fetched
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    @SuppressWarnings("unused")
    public List<T> getOnGets(List<Get> gets) throws IOException {
        List<T> records = new ArrayList<>(gets.size());
        try (Table table = getHBaseTable()) {
            Result[] results = table.get(gets);
            for (Result result : results) {
                records.add(hbObjectMapper.readValue(result, hbRecordClass));
            }
        }
        return records;
    }


    /**
     * Get specified number of versions of rows from HBase table by array of row keys (This method is a bulk variant of {@link #get(Serializable, int) get(R, int)} method)
     *
     * @param rowKeys            Row keys to fetch
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T[] get(R[] rowKeys, int numVersionsToFetch) throws IOException {
        List<Get> gets = new ArrayList<>(rowKeys.length);
        for (R rowKey : rowKeys) {
            gets.add(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
        }
        @SuppressWarnings("unchecked") T[] records = (T[]) Array.newInstance(hbRecordClass, rowKeys.length);
        try (Table table = getHBaseTable()) {
            Result[] results = table.get(gets);
            for (int i = 0; i < records.length; i++) {
                records[i] = hbObjectMapper.readValue(rowKeys[i], results[i], hbRecordClass);
            }
        }
        return records;
    }

    /**
     * Get records by array of row keys (This method is a bulk variant of {@link #get(Serializable) get(R)} method)
     *
     * @param rowKeys Row keys to fetch
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T[] get(R[] rowKeys) throws IOException {
        return get(rowKeys, 1);
    }

    /**
     * Get specified number of versions of rows from HBase table by list of row keys (This method is a multi-version variant of {@link #get(List)} method)
     *
     * @param rowKeys            Row keys to fetch
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Array of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> get(List<R> rowKeys, int numVersionsToFetch) throws IOException {
        List<Get> gets = new ArrayList<>(rowKeys.size());
        for (R rowKey : rowKeys) {
            gets.add(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
        }
        List<T> records = new ArrayList<>(rowKeys.size());
        try (Table table = getHBaseTable()) {
            Result[] results = table.get(gets);
            for (Result result : results) {
                records.add(hbObjectMapper.readValue(result, hbRecordClass));
            }
        }
        return records;
    }

    /**
     * Get records by list of row keys (This method is a bulk variant of {@link #get(Serializable) get(R)} method)
     *
     * @param rowKeys Row keys to fetch
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> get(List<R> rowKeys) throws IOException {
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
     * @throws IOException When HBase call fails
     */
    public List<T> get(R startRowKey, R endRowKey, int numVersionsToFetch) throws IOException {
        Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey))
                .withStopRow(toBytes(endRowKey))
                .readVersions(numVersionsToFetch);
        return get(scan);
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
     * @throws IOException When HBase call fails
     */
    public List<T> get(R startRowKey, boolean startRowInclusive, R endRowKey, boolean endRowInclusive, int numVersionsToFetch) throws IOException {
        Scan scan = new Scan()
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
     * @throws IOException When HBase call fails
     */
    public List<T> get(Scan scan) throws IOException {
        List<T> records = new ArrayList<>();
        try (Table table = getHBaseTable();
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                records.add(hbObjectMapper.readValue(result, hbRecordClass));
            }
        }
        return records;
    }

    /**
     * Get records whose row keys match provided prefix
     * <br><br>
     * <b>Caution:</b> If you expect large number or rows for given row key prefix, do <u>not</u> use this method. Use the iterable variant {@link #recordsByPrefix(byte[], int)} instead.
     *
     * @param rowPrefix          Prefix to scan for
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Records corresponding to provided prefix, deserialized as list of objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> getByPrefix(byte[] rowPrefix, int numVersionsToFetch) throws IOException {
        Scan scan = new Scan()
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
     * @throws IOException When HBase call fails
     */
    public List<T> getByPrefix(byte[] rowPrefix) throws IOException {
        return getByPrefix(rowPrefix, 1);
    }

    /**
     * Get an iterable to iterate over records matching given {@link Scan} object
     *
     * @param scan HBase's scan object
     * @return An iterable to iterate over records matching the scan criteria
     * @throws IOException When HBase call fails
     */
    public Records<T> records(Scan scan) throws IOException {
        return new Records<>(connection, hbObjectMapper, hbRecordClass, hbTable.getName(), scan);
    }

    /**
     * Get an iterable to iterate over records matching given row key prefix
     *
     * @param rowPrefix Prefix to scan for
     * @return An iterable to iterate over records matching the scan criteria
     * @throws IOException When HBase call fails
     */
    public Records<T> recordsByPrefix(byte[] rowPrefix) throws IOException {
        return recordsByPrefix(rowPrefix, 1);
    }

    /**
     * Get an iterable to iterate over records matching given row key prefix and fetch specific number of versions
     *
     * @param rowPrefix          Prefix to scan for
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return An iterable over objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public Records<T> recordsByPrefix(byte[] rowPrefix, int numVersionsToFetch) throws IOException {
        Scan scan = new Scan()
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
     * @throws IOException When HBase call fails
     */
    public Iterable<T> records(R startRowKey, R endRowKey) throws IOException {
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
     * @throws IOException When HBase call fails
     * @see <a href="https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html#setCaching-int-">HBase Scan caching</a>
     */
    public Records<T> records(R startRowKey, boolean startRowInclusive, R endRowKey, boolean endRowInclusive, int numVersionsToFetch, int numRowsForCaching) throws IOException {
        Scan scan = new Scan()
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
     * @throws IOException When HBase call fails
     */
    public Records<T> records(R startRowKey, R endRowKey, int numVersionsToFetch) throws IOException {
        Scan scan = new Scan()
                .withStartRow(toBytes(startRowKey))
                .withStopRow(toBytes(endRowKey))
                .readVersions(numVersionsToFetch);
        return records(scan);
    }

    private WrappedHBColumn validateAndGetLongColumn(String fieldName) {
        Field field = getField(fieldName);
        if (!Long.class.equals(field.getType())) {
            throw new IllegalArgumentException(String.format("Invalid attempt to increment a non-Long field (%s.%s)", hbRecordClass.getName(), fieldName));
        }
        return new WrappedHBColumn(field, true);
    }

    /**
     * Increments field by specified amount
     *
     * @param rowKey    Row key of the record whose column needs to be incremented
     * @param fieldName Field that needs to be incremented (this must be of {@link Long} type)
     * @param amount    Amount by which the HBase column needs to be incremented
     * @return The new value, post increment
     * @throws IOException When HBase call fails
     */
    public long increment(R rowKey, String fieldName, long amount) throws IOException {
        WrappedHBColumn hbColumn = validateAndGetLongColumn(fieldName);
        try (Table table = getHBaseTable()) {
            return table.incrementColumnValue(toBytes(rowKey), hbColumn.familyBytes(), hbColumn.columnBytes(), amount);
        }
    }

    /**
     * Increments field by specified amount
     *
     * @param rowKey     Row key of the record whose column needs to be incremented
     * @param fieldName  Field that needs to be incremented (this must be of {@link Long} type)
     * @param amount     Amount by which the HBase column needs to be incremented
     * @param durability The persistence guarantee for this increment (see {@link Durability})
     * @return The new value, post increment
     * @throws IOException When HBase call fails
     */
    public long increment(R rowKey, String fieldName, long amount, Durability durability) throws IOException {
        WrappedHBColumn hbColumn = validateAndGetLongColumn(fieldName);
        try (Table table = getHBaseTable()) {
            return table.incrementColumnValue(toBytes(rowKey), hbColumn.familyBytes(), hbColumn.columnBytes(), amount, durability);
        }
    }

    /**
     * Gets (native) {@link Increment} object for given row key, to be later used in {@link #increment(Increment)} method.
     *
     * @param rowKey HBase row key
     * @return Increment object
     */
    public Increment getIncrement(R rowKey) {
        return new Increment(toBytes(rowKey));
    }

    /**
     * Performs HBase {@link Table#increment} on the given {@link Increment} object <br>
     * <br>
     * <b>Note</b>: <ul>
     * <li>You may construct {@link Increment} object using the {@link #getIncrement(Serializable) getIncrement} method</li>
     * <li>Unlike the {@link #increment(Serializable, String, long)} methods, this method skips some validations (hence, be cautious)</li>
     * </ul>
     *
     * @param increment HBase Increment object
     * @return <b>Partial object</b> containing (only) values  of fields that were incremented
     * @throws IOException When HBase call fails
     */
    public T increment(Increment increment) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.increment(increment);
            return hbObjectMapper.readValue(result, hbRecordClass);
        }
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
     * @throws IOException When HBase call fails
     * @see Table#append(Append)
     * @see #append(Serializable, Map)
     */
    public T append(R rowKey, String fieldName, Object valueToAppend) throws IOException {
        Map<String, Object> one = new HashMap<>(1);
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
     * @throws IOException When HBase call fails
     * @see Table#append(Append)
     * @see #append(Serializable, String, Object)
     */
    public T append(R rowKey, Map<String, Object> valuesToAppend) throws IOException {
        Append append = new Append(toBytes(rowKey));
        for (Map.Entry<String, Object> e : valuesToAppend.entrySet()) {
            String fieldName = e.getKey();
            Field field = getField(fieldName);
            WrappedHBColumn hbColumn = new WrappedHBColumn(field, true);
            Object value = e.getValue();
            if (!field.getType().isAssignableFrom(value.getClass())) {
                throw new IllegalArgumentException(String.format("An attempt was made to append a value of type '%s' to field '%s', which is of type '%s' (incompatible)", value.getClass(), fieldName, field.getType()));
            }
            append.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes(),
                    hbObjectMapper.valueToByteArray((Serializable) value, hbColumn.codecFlags())
            );
        }
        try (Table table = getHBaseTable()) {
            Result result = table.append(append);
            return hbObjectMapper.readValue(result, hbRecordClass);
        }
    }


    /**
     * Gets HBase's (native) {@link Append} object for given row key, to be later used in {@link #append(Append)} method.
     *
     * @param rowKey HBase row key
     * @return HBase's {@link Append} object
     */
    public Append getAppend(R rowKey) {
        return new Append(toBytes(rowKey));
    }

    /**
     * Performs HBase's {@link Table#append} on the given {@link Append} object <br>
     * <br>
     * <b>Note</b>: <ul>
     * <li>You may construct {@link Append} object using the {@link #getAppend(Serializable) getAppend} method</li>
     * <li>Unlike the {@link #append(Serializable, String, Object)} and related methods, this method skips some validations. So, use this only if you need access to HBase's native methods.</li>
     * </ul>
     *
     * @param append HBase's {@link Append} object
     * @return <b>Partial object</b> containing (only) values  of fields that were appended
     * @throws IOException When HBase call fails
     */
    public T append(Append append) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.append(append);
            return hbObjectMapper.readValue(result, hbRecordClass);
        }
    }

    /**
     * Get specified number of versions of rows by a range of row keys (start to end)
     *
     * @param startRowKey Row start
     * @param endRowKey   Row end
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> get(R startRowKey, R endRowKey) throws IOException {
        return get(startRowKey, endRowKey, 1);
    }

    /**
     * Persist your bean-like object (of a class that implements {@link HBRecord}) to HBase table
     *
     * @param record Object that needs to be persisted
     * @return Row key of the persisted object, represented as a {@link String}
     * @throws IOException When HBase call fails
     */
    public R persist(HBRecord<R> record) throws IOException {
        Put put = hbObjectMapper.writeValueAsPut(record);
        try (Table table = getHBaseTable()) {
            table.put(put);
            return record.composeRowKey();
        }
    }

    /**
     * Persist a list of your bean-like objects (of a class that implements {@link HBRecord}) to HBase table (this is a bulk variant of {@link #persist(HBRecord)} method)
     *
     * @param records List of objects that needs to be persisted
     * @return Row keys of the persisted objects, represented as a {@link String}
     * @throws IOException When HBase call fails
     */
    public List<R> persist(List<T> records) throws IOException {
        List<Put> puts = new ArrayList<>(records.size());
        List<R> rowKeys = new ArrayList<>(records.size());
        for (HBRecord<R> object : records) {
            puts.add(hbObjectMapper.writeValueAsPut(object));
            rowKeys.add(object.composeRowKey());
        }
        try (Table table = getHBaseTable()) {
            table.put(puts);
        }
        return rowKeys;
    }


    /**
     * Delete a row from an HBase table for a given row key
     *
     * @param rowKey row key to delete
     * @throws IOException When HBase call fails
     */
    public void delete(R rowKey) throws IOException {
        Delete delete = new Delete(toBytes(rowKey));
        try (Table table = getHBaseTable()) {
            table.delete(delete);
        }
    }

    /**
     * Delete HBase row by object (of class that implements {@link HBRecord}
     *
     * @param record Object to delete
     * @throws IOException When HBase call fails
     */
    public void delete(HBRecord<R> record) throws IOException {
        this.delete(record.composeRowKey());
    }

    /**
     * Delete HBase rows for an array of row keys
     *
     * @param rowKeys row keys to delete
     * @throws IOException When HBase call fails
     */
    public void delete(R[] rowKeys) throws IOException {
        List<Delete> deletes = new ArrayList<>(rowKeys.length);
        for (R rowKey : rowKeys) {
            deletes.add(new Delete(toBytes(rowKey)));
        }
        try (Table table = getHBaseTable()) {
            table.delete(deletes);
        }
    }

    /**
     * Delete HBase rows by object references
     *
     * @param records Records to delete
     * @throws IOException When HBase call fails
     */
    public void delete(List<T> records) throws IOException {
        List<Delete> deletes = new ArrayList<>(records.size());
        for (HBRecord<R> record : records) {
            deletes.add(new Delete(toBytes(record.composeRowKey())));
        }
        try (Table table = getHBaseTable()) {
            table.delete(deletes);
        }
    }

    /**
     * Get HBase table name
     *
     * @return Name of table read as String
     */
    public String getTableName() {
        return hbRecordClass.getAnnotation(HBTable.class).name();
    }

    /**
     * Get the mapped column families and their versions (as specified in {@link HBTable} annotation)
     *
     * @return A {@link Map} containing names of column families as mapped in the entity class and number of versions
     */
    public Map<String, Integer> getColumnFamiliesAndVersions() {
        return hbObjectMapper.getColumnFamiliesAndVersions(hbRecordClass);
    }

    /**
     * Get list of fields (private variables of your bean-like class)
     *
     * @return A {@link Set} containing names of fields
     */
    public Set<String> getFields() {
        return fields.keySet();
    }


    /**
     * Get reference to HBase table
     *
     * @return {@link HTable} object
     * @throws IOException When table reference couldn't be resolved through connection
     */
    public Table getHBaseTable() throws IOException {
        return connection.getTable(hbTable.getName());
    }

    private Field getField(String fieldName) {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Unrecognized field: '%s'. Choose one of %s%n", fieldName, fields.keySet()));
        }
        return field;
    }

    private void populateFieldValuesToMap(Field field, Result result, Map<R, NavigableMap<Long, Object>> map) {
        if (result.isEmpty()) {
            return;
        }
        WrappedHBColumn hbColumn = new WrappedHBColumn(field, true);
        List<Cell> cells = result.getColumnCells(hbColumn.familyBytes(), hbColumn.columnBytes());
        for (Cell cell : cells) {
            Type fieldType = hbObjectMapper.getFieldType(field, hbColumn.isMultiVersioned());
            @SuppressWarnings("unchecked") final R rowKey = hbObjectMapper.bytesToRowKey(CellUtil.cloneRow(cell), hbTable.getCodecFlags(), (Class<T>) field.getDeclaringClass());
            if (!map.containsKey(rowKey)) {
                map.put(rowKey, new TreeMap<>());
            }
            map.get(rowKey).put(cell.getTimestamp(), hbObjectMapper.byteArrayToValue(CellUtil.cloneValue(cell), fieldType, hbColumn.codecFlags()));
        }
    }

    /**
     * Fetch value of column for a given row key and field
     *
     * @param rowKey    Row key to reference HBase row
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Value of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     * @throws IOException When HBase call fails
     */
    public Object fetchFieldValue(R rowKey, String fieldName) throws IOException {
        final NavigableMap<Long, Object> fieldValues = fetchFieldValue(rowKey, fieldName, 1);
        if (fieldValues == null || fieldValues.isEmpty()) {
            return null;
        } else {
            return fieldValues.lastEntry().getValue();
        }
    }


    /**
     * Fetch multiple versions of column values by row key and field name
     *
     * @param rowKey             Row key to reference HBase row
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return {@link NavigableMap} of timestamps and values of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     * @throws IOException When HBase call fails
     */
    public NavigableMap<Long, Object> fetchFieldValue(R rowKey, String fieldName, int numVersionsToFetch) throws IOException {
        @SuppressWarnings("unchecked") R[] array = (R[]) Array.newInstance(rowKeyClass, 1);
        array[0] = rowKey;
        return fetchFieldValues(array, fieldName, numVersionsToFetch).get(rowKey);

    }

    /**
     * Fetch values of an HBase column for a range of row keys (start and end) and field name
     *
     * @param startRowKey Start row key (scan start)
     * @param endRowKey   End row key (scan end)
     * @param fieldName   Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Map of row key and column value
     * @throws IOException When HBase call fails
     */
    public Map<R, Object> fetchFieldValues(R startRowKey, R endRowKey, String fieldName) throws IOException {
        final Map<R, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValues(startRowKey, endRowKey, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, 10);
    }

    private Map<R, Object> toSingleVersioned(Map<R, NavigableMap<Long, Object>> multiVersionedMap, int mapInitialCapacity) {
        Map<R, Object> map = new HashMap<>(mapInitialCapacity, 1.0f);
        for (Map.Entry<R, NavigableMap<Long, Object>> e : multiVersionedMap.entrySet()) {
            map.put(e.getKey(), e.getValue().lastEntry().getValue());
        }
        return map;
    }

    /**
     * Fetch specified number of versions of values of an HBase column for a range of row keys (start and end) and field name
     *
     * @param startRowKey        Start row key (scan start)
     * @param endRowKey          End row key (scan end)
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Map of row key and column values (versioned)
     * @throws IOException When HBase call fails
     */
    public NavigableMap<R, NavigableMap<Long, Object>> fetchFieldValues(R startRowKey, R endRowKey, String fieldName, int numVersionsToFetch) throws IOException {
        Field field = getField(fieldName);
        WrappedHBColumn hbColumn = new WrappedHBColumn(field, true);
        Scan scan = new Scan().withStartRow(toBytes(startRowKey)).withStopRow(toBytes(endRowKey));
        scan.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes());
        scan.readVersions(numVersionsToFetch);
        NavigableMap<R, NavigableMap<Long, Object>> map = new TreeMap<>();
        try (Table table = getHBaseTable();
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                populateFieldValuesToMap(field, result, map);
            }
        }
        return map;
    }

    /**
     * Fetch column values for a given array of row keys (bulk variant of method {@link #fetchFieldValue(Serializable, String)})
     *
     * @param rowKeys   Array of row keys to fetch
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Map of row key and column values
     * @throws IOException Exception from HBase
     */
    public Map<R, Object> fetchFieldValues(R[] rowKeys, String fieldName) throws IOException {
        final Map<R, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValues(rowKeys, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, rowKeys.length);
    }

    /**
     * Fetch specified number of versions of values of an HBase column for an array of row keys
     *
     * @param rowKeys            Array of row keys to fetch
     * @param fieldName          Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param numVersionsToFetch Number of versions to be retrieved
     * @return Map of row key and column values (versioned)
     * @throws IOException When HBase call fails
     */
    public Map<R, NavigableMap<Long, Object>> fetchFieldValues(R[] rowKeys, String fieldName, int numVersionsToFetch) throws IOException {
        Field field = getField(fieldName);
        WrappedHBColumn hbColumn = new WrappedHBColumn(field, true);
        List<Get> gets = new ArrayList<>(rowKeys.length);
        for (R rowKey : rowKeys) {
            Get get = new Get(toBytes(rowKey));
            get.readVersions(numVersionsToFetch);
            get.addColumn(hbColumn.familyBytes(), hbColumn.columnBytes());
            gets.add(get);
        }
        Map<R, NavigableMap<Long, Object>> map = new HashMap<>(rowKeys.length, 1.0f);
        try (Table table = getHBaseTable()) {
            Result[] results = table.get(gets);
            for (Result result : results) {
                populateFieldValuesToMap(field, result, map);
            }
        }
        return map;
    }

    /**
     * Convert typed row key into a byte array
     *
     * @param rowKey Row key, as used in your code
     * @return Byte array corresponding to HBase row key
     */
    public byte[] toBytes(R rowKey) {
        return hbObjectMapper.rowKeyToBytes(rowKey, hbTable.getCodecFlags());
    }
}
