package com.flipkart.hbaseobjectmapper;

import com.google.common.reflect.TypeToken;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Base capabilities for HBase DAO.
 *
 * @param <R> Data type of row key, which should be '{@link Comparable} with itself' and must be {@link Serializable} (e.g. {@link String}, {@link Integer}, {@link BigDecimal} etc. or your own POJO)
 * @param <T> Entity type that maps to an HBase row (this type must have implemented {@link HBRecord} interface)
 */
abstract class BaseHBDAO<R extends Serializable & Comparable<R>, T extends HBRecord<R>> {
    protected final HBObjectMapper hbObjectMapper;
    protected final Class<R> rowKeyClass;
    protected final Class<T> hbRecordClass;
    protected final WrappedHBTable<R, T> hbTable;
    private final Map<String, Field> fields;

    @SuppressWarnings({"unchecked", "UnstableApiUsage"})
    protected BaseHBDAO(final HBObjectMapper hbObjectMapper) {
        this.hbObjectMapper = hbObjectMapper;
        hbRecordClass = (Class<T>) new TypeToken<T>(getClass()) {
        }.getRawType();
        if (hbRecordClass == null) {
            throw new IllegalStateException("Unable to resolve HBase record type");
        }
        this.hbObjectMapper.validateHBClass(hbRecordClass);
        this.rowKeyClass = (Class<R>) new TypeToken<R>(getClass()) {
        }.getRawType();
        if (rowKeyClass == null) {
            throw new IllegalStateException("Unable to resolve HBase rowkey type");
        }
        this.hbTable = new WrappedHBTable<>(hbRecordClass);
        this.fields = hbObjectMapper.getHBColumnFields0(hbRecordClass);
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
     * Convert typed row key into a byte array
     *
     * @param rowKey Row key, as used in your code
     * @return Byte array corresponding to HBase row key
     */
    public byte[] toBytes(R rowKey) {
        return hbObjectMapper.rowKeyToBytes(rowKey, hbTable.getCodecFlags());
    }

    /**
     * Creates an HBase {@link Get} object, for enabling specialised read of HBase rows.
     * <br><br>
     *
     * @param rowKey Row key
     * @return HBase's Get object
     */
    public Get getGet(@Nonnull final R rowKey) {
        return new Get(toBytes(rowKey));
    }

    /**
     * Gets (native) {@link Increment} object for given row key, to be later used in increment method.
     *
     * @param rowKey HBase row key
     * @return Increment object
     */
    public Increment getIncrement(@Nonnull final R rowKey) {
        return new Increment(toBytes(rowKey));
    }

    /**
     * Gets HBase's (native) {@link Append} object for given row key, to be later used in append method.
     *
     * @param rowKey HBase row key
     * @return HBase's {@link Append} object
     */
    public Append getAppend(R rowKey) {
        return new Append(toBytes(rowKey));
    }

    protected void populateFieldValuesToMap(final Field field, final Result result, final Map<R, NavigableMap<Long, Object>> map) {
        if (result.isEmpty()) {
            return;
        }
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
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

    protected WrappedHBColumn validateAndGetLongColumn(@Nonnull final String fieldName) {
        Field field = getField(fieldName);
        if (!Long.class.equals(field.getType())) {
            throw new IllegalArgumentException(String.format("Invalid attempt to increment a non-Long field (%s.%s)", hbRecordClass.getName(), fieldName));
        }
        return new WrappedHBColumn(field);
    }

    protected Field getField(@Nonnull final String fieldName) {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Unrecognized field: '%s'. Choose one of %s%n", fieldName, fields.keySet()));
        }
        return field;
    }

    protected Map<R, Object> toSingleVersioned(@Nonnull final Map<R, NavigableMap<Long, Object>> multiVersionedMap, final int mapInitialCapacity) {
        final Map<R, Object> map = new HashMap<>(mapInitialCapacity, 1.0f);
        for (final Map.Entry<R, NavigableMap<Long, Object>> e : multiVersionedMap.entrySet()) {
            map.put(e.getKey(), e.getValue().lastEntry().getValue());
        }
        return map;
    }

    protected Function<Result, T> mapResultToRecordType() {
        return result -> hbObjectMapper.readValueFromResult(result, hbRecordClass);
    }
}
