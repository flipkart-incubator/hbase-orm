package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.DeserializationException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldNotMappedToHBaseColumnException;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

/**
 * A <i>Data Access Object</i> class that enables simpler random access of HBase rows
 *
 * @param <T> Entity type that maps to an HBase row (type must implement {@link HBRecord})
 */
public abstract class AbstractHBDAO<T extends HBRecord> {

    /**
     * Default number of versions to fetch
     */
    private static final int DEFAULT_NUM_VERSIONS = 1;
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
     * Get specified number of versions of a row from HBase table by it's row key
     *
     * @param rowKey   Row key
     * @param versions Number of versions to be retrieved
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(String rowKey, int versions) throws IOException {
        Result result = this.hTable.get(new Get(Bytes.toBytes(rowKey)).setMaxVersions(versions));
        return hbObjectMapper.readValue(rowKey, result, hbRecordClass);
    }

    /**
     * Get a row from HBase table by it's row key
     *
     * @param rowKey Row key
     * @return HBase row, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T get(String rowKey) throws IOException {
        return get(rowKey, DEFAULT_NUM_VERSIONS);
    }


    /**
     * Get specified number of versions of rows from HBase table by array of row keys (This method is a bulk variant of {@link #get(String, int)} method)
     *
     * @param rowKeys  Row keys to fetch
     * @param versions Number of versions of columns to fetch
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
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
     * Get rows from HBase table by array of row keys (This method is a bulk variant of {@link #get(String)} method)
     *
     * @param rowKeys Row keys to fetch
     * @return Array of HBase rows, deserialized as object of your bean-like class (that implements {@link HBRecord})
     * @throws IOException When HBase call fails
     */
    public T[] get(String[] rowKeys) throws IOException {
        return get(rowKeys, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Get specified number of versions of rows from HBase table by list of row keys (This method is a multi-version variant of {@link #get(List)} method)
     *
     * @param rowKeys  Row keys to fetch
     * @param versions Number of versions of columns to fetch
     * @return Array of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
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
     * Get rows from HBase table by list of row keys (This method is a bulk variant of {@link #get(String)} method)
     *
     * @param rowKeys Row keys to fetch
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> get(List<String> rowKeys) throws IOException {
        return get(rowKeys, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Get specified number of versions of rows from HBase table by a range of row keys (start and end) - this is a multi-version variant of {@link #get(String, String)}
     *
     * @param startRowKey Row start
     * @param endRowKey   Row end
     * @param versions    Number of versions to fetch
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
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
     * Get specified number of versions of rows from HBase table by a range of row keys (start to end)
     *
     * @param startRowKey Row start
     * @param endRowKey   Row end
     * @return List of rows corresponding to row keys passed, deserialized as objects of your bean-like class
     * @throws IOException When HBase call fails
     */
    public List<T> get(String startRowKey, String endRowKey) throws IOException {
        return get(startRowKey, endRowKey, DEFAULT_NUM_VERSIONS);
    }

    /**
     * Persist your bean-like object (of a class that implements {@link HBRecord}) to HBase table
     *
     * @param object Object that needs to be persisted
     * @return Row key of the persisted object, represented as a {@link String}
     * @throws IOException When HBase call fails
     */
    public String persist(HBRecord object) throws IOException {
        Put put = hbObjectMapper.writeValueAsPut(object);
        hTable.put(put);
        return object.composeRowKey();
    }

    /**
     * Persist a list of your bean-like objects (of a class that implements {@link HBRecord}) to HBase table (this is a bulk variant of {@link #persist(HBRecord)} method)
     *
     * @param objects List of objects that needs to be persisted
     * @return Row keys of the persisted objects, represented as a {@link String}
     * @throws IOException When HBase call fails
     */
    public List<String> persist(List<? extends HBRecord> objects) throws IOException {
        List<Put> puts = new ArrayList<Put>(objects.size());
        List<String> rowKeys = new ArrayList<String>(objects.size());
        for (HBRecord object : objects) {
            puts.add(hbObjectMapper.writeValueAsPut(object));
            rowKeys.add(object.composeRowKey());
        }
        hTable.put(puts);
        return rowKeys;
    }


    /**
     * Delete a row from an HBase table for a given row key
     *
     * @param rowKey row key to delete
     * @throws IOException When HBase call fails
     */
    public void delete(String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        this.hTable.delete(delete);
    }

    /**
     * Delete HBase row by object (of class that implements {@link HBRecord}
     *
     * @param object Object to delete
     * @throws IOException When HBase call fails
     */
    public void delete(HBRecord object) throws IOException {
        this.delete(object.composeRowKey());
    }

    /**
     * Delete HBase rows for an array of row keys
     *
     * @param rowKeys row keys to delete
     * @throws IOException When HBase call fails
     */
    public void delete(String[] rowKeys) throws IOException {
        List<Delete> deletes = new ArrayList<Delete>(rowKeys.length);
        for (String rowKey : rowKeys) {
            deletes.add(new Delete(Bytes.toBytes(rowKey)));
        }
        this.hTable.delete(deletes);
    }

    /**
     * Delete HBase rows by object references
     *
     * @param objects Objects to delete
     * @throws IOException When HBase call fails
     */
    public void delete(HBRecord[] objects) throws IOException {
        String[] rowKeys = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            rowKeys[i] = objects[i].composeRowKey();
        }
        this.delete(rowKeys);
    }

    /**
     * Get HBase table name
     *
     * @return Name of table read as String
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

    private static void populateFieldValuesToMap(Field field, Result result, Map<String, NavigableMap<Long, Object>> map) throws DeserializationException {
        if (result.isEmpty())
            return;
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        List<Cell> cells = result.getColumnCells(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
        for (Cell cell : cells) {
            Type fieldType = hbColumn.isMultiVersioned() ? hbObjectMapper.getComponentType(field) : field.getType();
            final String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            if (!map.containsKey(rowKey))
                map.put(rowKey, new TreeMap<Long, Object>());
            map.get(rowKey).put(cell.getTimestamp(), hbObjectMapper.byteArrayToValue(CellUtil.cloneValue(cell), fieldType, hbColumn.serializeAsString()));
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
    public Object fetchFieldValue(String rowKey, String fieldName) throws IOException {
        final NavigableMap<Long, Object> fieldValues = fetchFieldValue(rowKey, fieldName, 1);
        if (fieldValues == null || fieldValues.isEmpty()) return null;
        else return fieldValues.lastEntry().getValue();
    }


    /**
     * Fetch multiple versions of column values by row key and field name
     *
     * @param rowKey    Row key to reference HBase row
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param versions  Number of versions of column to fetch
     * @return {@link NavigableMap} of timestamps and values of the column (boxed), <code>null</code> if row with given rowKey doesn't exist or such field doesn't exist for the row
     * @throws IOException When HBase call fails
     */
    public NavigableMap<Long, Object> fetchFieldValue(String rowKey, String fieldName, int versions) throws IOException {
        return fetchFieldValues(new String[]{rowKey}, fieldName, versions).get(rowKey);
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
    public Map<String, Object> fetchFieldValues(String startRowKey, String endRowKey, String fieldName) throws IOException {
        final Map<String, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValues(startRowKey, endRowKey, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, 10);
    }

    private static Map<String, Object> toSingleVersioned(Map<String, NavigableMap<Long, Object>> multiVersionedMap, int mapInitialCapacity) {
        Map<String, Object> map = new HashMap<String, Object>(mapInitialCapacity);
        for (Map.Entry<String, NavigableMap<Long, Object>> e : multiVersionedMap.entrySet()) {
            map.put(e.getKey(), e.getValue().lastEntry().getValue());
        }
        return map;
    }

    /**
     * Fetch specified number of versions of values of an HBase column for a range of row keys (start and end) and field name
     *
     * @param startRowKey Start row key (scan start)
     * @param endRowKey   End row key (scan end)
     * @param fieldName   Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param versions    Number of versions of column to fetch
     * @return Map of row key and column values (versioned)
     * @throws IOException When HBase call fails
     */
    public NavigableMap<String, NavigableMap<Long, Object>> fetchFieldValues(String startRowKey, String endRowKey, String fieldName, int versions) throws IOException {
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
     *
     * @param rowKeys   Array of row keys to fetch
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @return Map of row key and column values
     * @throws IOException
     */
    public Map<String, Object> fetchFieldValues(String[] rowKeys, String fieldName) throws IOException {
        final Map<String, NavigableMap<Long, Object>> multiVersionedMap = fetchFieldValues(rowKeys, fieldName, 1);
        return toSingleVersioned(multiVersionedMap, rowKeys.length);
    }

    /**
     * Fetch specified number of versions of values of an HBase column for an array of row keys
     *
     * @param rowKeys   Array of row keys to fetch
     * @param fieldName Name of the private variable of your bean-like object (of a class that implements {@link HBRecord}) whose corresponding column needs to be fetched
     * @param versions  Number of versions of column to fetch
     * @return Map of row key and column values (versioned)
     * @throws IOException When HBase call fails
     */
    public Map<String, NavigableMap<Long, Object>> fetchFieldValues(String[] rowKeys, String fieldName, int versions) throws IOException {
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
