package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.exceptions.FieldNotMappedToHBaseColumnException;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * A <i>Data Access Object</i> class that enables simpler random access of HBase rows
 *
 * @param <T> Entity type that maps to an HBase row (type must implement {@link HBRecord})
 */
public abstract class AbstractHBDAO<T extends HBRecord> {

    public static final int DEFAULT_NUM_VERSIONS = 1;
    protected static final HBObjectMapper hbObjectMapper = new HBObjectMapper();
    protected final HTable hTable;
    @SuppressWarnings("FieldCanBeLocal")
    private final TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    protected final Class<T> hbRecordClass;
    protected final Map<String, Field> fields;

    /**
     * Constructs a data access object. Classes extending this class <strong>must</strong> call this constructor using <code>super</code>
     *
     * @param conf Hadoop configuration
     */
    @SuppressWarnings("unchecked")
    protected AbstractHBDAO(Configuration conf) throws IOException {
        hbRecordClass = (Class<T>) typeToken.getRawType();
        if (hbRecordClass == null || hbRecordClass == HBRecord.class)
            throw new IllegalStateException("Unable to resolve HBase record type (record class is resolving to " + hbRecordClass + ")");
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        if (hbTable == null)
            throw new IllegalStateException(String.format("Type %s should be annotated with %s for use in class %s", hbRecordClass.getName(), HBTable.class.getName(), AbstractHBDAO.class.getName()));
        this.hTable = new HTable(conf, hbTable.value());
        this.fields = hbObjectMapper.getHBFields(hbRecordClass);
    }

    /**
     * Get one row from HBase table by it's row key
     *
     * @param rowKey   Row key
     * @param versions Number of versions to be retrieved (default value: {@link #DEFAULT_NUM_VERSIONS})
     * @return Contents of one row read as your bean-like object (of a class that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(String rowKey, int versions) throws IOException {
        Result result = this.hTable.get(new Get(Bytes.toBytes(rowKey)).setMaxVersions(versions));
        return hbObjectMapper.readValue(rowKey, result, hbRecordClass);
    }

    /**
     * Get one row from HBase table by it's row key
     *
     * @param rowKey Row key
     * @return Contents of one row read as your bean-like object (of a class that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(String rowKey) throws IOException {
        return get(rowKey, DEFAULT_NUM_VERSIONS);
    }


    /**
     * Get multiple rows from HBase table for an array of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public T[] get(String[] rowKeys, int versions) throws IOException {
        List<Get> gets = new ArrayList<Get>(rowKeys.length);
        for (String rowKey : rowKeys) {
            gets.add(new Get(Bytes.toBytes(rowKey)).setMaxVersions(versions));
        }
        Result[] results = this.hTable.get(gets);
        @SuppressWarnings("unchecked") T[] records = (T[]) Array.newInstance(hbRecordClass, rowKeys.length);
        for (int i = 0; i < records.length; i++) {
            records[i] = hbObjectMapper.readValue(rowKeys[i], results[i], hbRecordClass);
        }
        return records;
    }

    /**
     * Get multiple rows from HBase table in one shot for an array of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public T[] get(String[] rowKeys) throws IOException {
        return get(rowKeys, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Get multiple rows from HBase table in one shot for an array of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public List<T> get(List<String> rowKeys, int versions) throws IOException {
        List<Get> gets = new ArrayList<Get>(rowKeys.size());
        for (String rowKey : rowKeys) {
            gets.add(new Get(Bytes.toBytes(rowKey)).setMaxVersions(versions));
        }
        Result[] results = this.hTable.get(gets);
        List<T> records = new ArrayList<T>(rowKeys.size());
        for (Result result : results) {
            records.add(hbObjectMapper.readValue(result, hbRecordClass));
        }
        return records;
    }

    /**
     * Get multiple rows from HBase table in one shot for an array of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public List<T> get(List<String> rowKeys) throws IOException {
        return get(rowKeys, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Get multiple rows from HBase table in one shot for a range of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public List<T> get(String startRowKey, String endRowKey, int versions) throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey)).setMaxVersions(versions);
        ResultScanner scanner = hTable.getScanner(scan);
        List<T> records = new ArrayList<T>();
        for (Result result : scanner) {
            records.add(hbObjectMapper.readValue(result, hbRecordClass));
        }
        return records;
    }

    /**
     * Get multiple rows from HBase table in one shot for a range of row keys (This API is a bulk variant of {@link #get(String)} method)
     */
    public List<T> get(String startRowKey, String endRowKey) throws IOException {
        return get(startRowKey, endRowKey, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Persist your bean-like object (of a class that implements {@link HBRecord}) to HBase table
     *
     * @param obj Object that needs to be persisted
     * @return Row key for the object
     * @throws IOException Thrown if there is an HBase error
     */
    public String persist(HBRecord obj) throws IOException {
        Put put = hbObjectMapper.writeValueAsPut(obj);
        hTable.put(put);
        return obj.composeRowKey();
    }

    /**
     * Persist a list of your bean-like objects (of a class that implements {@link HBRecord}) to HBase table (this is a bulk variant of {@link #persist(HBRecord)} method)
     */
    public List<String> persist(List<? extends HBRecord> objs) throws IOException {
        List<Put> puts = new ArrayList<Put>(objs.size());
        List<String> rowKeys = new ArrayList<String>(objs.size());
        for (HBRecord obj : objs) {
            puts.add(hbObjectMapper.writeValueAsPut(obj));
            rowKeys.add(obj.composeRowKey());
        }
        hTable.put(puts);
        return rowKeys;
    }


    /**
     * Delete row from an HBase table for a given row key
     */
    public void delete(String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        this.hTable.delete(delete);
    }

    /**
     * Delete HBase row by row key
     */
    public void delete(HBRecord obj) throws IOException {
        this.delete(obj.composeRowKey());
    }

    /**
     * Delete HBase rows for an array of row keys
     */
    public void delete(String[] rowKeys) throws IOException {
        List<Delete> deletes = new ArrayList<Delete>(rowKeys.length);
        for (String rowKey : rowKeys) {
            deletes.add(new Delete(Bytes.toBytes(rowKey)));
        }
        this.hTable.delete(deletes);
    }

    /**
     * Delete HBase rows by referencing objects
     */
    public void delete(HBRecord[] objs) throws IOException {
        String[] rowKeys = new String[objs.length];
        for (int i = 0; i < objs.length; i++) {
            rowKeys[i] = objs[i].composeRowKey();
        }
        this.delete(rowKeys);
    }

    /**
     * Get HBase table name
     */
    public String getTableName() {
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        return hbTable.value();
    }

    /**
     * Get list of column families mapped
     */
    public Set<String> getColumnFamilies() {
        return hbObjectMapper.getColumnFamilies(hbRecordClass);
    }

    /**
     * Get list of fields (private variables of your bean-like class)
     */
    public Set<String> getFields() {
        return fields.keySet();
    }


    /**
     * Get reference to HBase table
     *
     * @return {@link HTable} object
     */
    public HTable getHBaseTable() {
        return hTable;
    }

    private Field getField(String fieldName) {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Unrecognized field: '%s'. Choose one of %s", fieldName, fields.values().toString()));
        }
        return field;
    }

    private static void populateFieldValuesToMap(Field field, Result result, Map<String, NavigableMap<Long, Object>> map) {
        if (result.isEmpty())
            return;
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        List<KeyValue> kvs = result.getColumn(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
        for (KeyValue kv : kvs) {
            Class<?> fieldType = hbColumn.isMultiVersioned() ? (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1] : field.getType();
            final String rowKey = Bytes.toString(kv.getRow());
            if (!map.containsKey(rowKey))
                map.put(rowKey, new TreeMap<Long, Object>());
            map.get(rowKey).put(kv.getTimestamp(), hbObjectMapper.byteArrayToValue(kv.getValue(), fieldType, hbColumn.serializeAsString()));
        }
    }

    /**
     * Fetch value of column for a given row key and field
     *
     * @param rowKey    Row key to reference HBase row
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord})
     * @return Value of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     * @throws IOException Thrown when there is an exception from HBase
     */
    public Object fetchFieldValue(String rowKey, String fieldName) throws IOException {
        final NavigableMap<Long, Object> fieldValues = fetchFieldValueVersioned(rowKey, fieldName, 1);
        if (fieldValues == null || fieldValues.isEmpty()) return null;
        else return fieldValues.lastEntry().getValue();
    }


    public NavigableMap<Long, Object> fetchFieldValueVersioned(String rowKey, String fieldName, int versions) throws IOException {
        return fetchFieldValuesVersioned(new String[]{rowKey}, fieldName, versions).get(rowKey);
    }

    /**
     * Fetch column values for a given range of row keys (bulk variant of method {@link #fetchFieldValue(String, String)})
     */
    public Map<String, Object> fetchFieldValues(String startRowKey, String endRowKey, String fieldName) throws IOException {
        final Map<String, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValuesVersioned(startRowKey, endRowKey, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, 10);
    }

    private static Map<String, Object> toSingleVersioned(Map<String, NavigableMap<Long, Object>> multiVersionedMap, int mapInitialCapacity) {
        Map<String, Object> map = new HashMap<String, Object>(mapInitialCapacity);
        for (Map.Entry<String, NavigableMap<Long, Object>> e : multiVersionedMap.entrySet()) {
            map.put(e.getKey(), e.getValue().lastEntry().getValue());
        }
        return map;
    }

    public NavigableMap<String, NavigableMap<Long, Object>> fetchFieldValuesVersioned(String startRowKey, String endRowKey, String fieldName, int versions) throws IOException {
        Field field = getField(fieldName);
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));
        scan.addColumn(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
        scan.setMaxVersions(versions);
        ResultScanner scanner = hTable.getScanner(scan);
        NavigableMap<String, NavigableMap<Long, Object>> map = new TreeMap<String, NavigableMap<Long, Object>>();
        for (Result result : scanner) {
            populateFieldValuesToMap(field, result, map);
        }
        return map;
    }

    /**
     * Fetch column values for a given array of row keys (bulk variant of method {@link #fetchFieldValue(String, String)})
     */
    public Map<String, Object> fetchFieldValues(String[] rowKeys, String fieldName) throws IOException {
        final Map<String, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValuesVersioned(rowKeys, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, rowKeys.length);
    }

    public Map<String, NavigableMap<Long, Object>> fetchFieldValuesVersioned(String[] rowKeys, String fieldName, int versions) throws IOException {
        Field field = getField(fieldName);
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        if (!hbColumn.isPresent()) {
            throw new FieldNotMappedToHBaseColumnException(hbRecordClass, fieldName);
        }
        List<Get> gets = new ArrayList<Get>(rowKeys.length);
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setMaxVersions(versions);
            get.addColumn(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
            gets.add(get);
        }
        Result[] results = this.hTable.get(gets);
        Map<String, NavigableMap<Long, Object>> map = new HashMap<String, NavigableMap<Long, Object>>(rowKeys.length);
        for (Result result : results) {
            populateFieldValuesToMap(field, result, map);
        }
        return map;
    }

}
