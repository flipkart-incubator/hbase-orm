package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * An object mapper class that helps convert your bean-like objects to HBase's {@link Put} and {@link Result} objects (and vice-versa). For use in Map/Reduce jobs and their unit-tests
 */
public class HBObjectMapper {

    private static final Map<Class, String> fromBytesMethodNames = new HashMap<Class, String>() {
        {
            put(Boolean.class, "toBoolean");
            put(Short.class, "toShort");
            put(Integer.class, "toInt");
            put(Long.class, "toLong");
            put(Float.class, "toFloat");
            put(Double.class, "toDouble");
            put(String.class, "toString");
            put(BigDecimal.class, "toBigDecimal");
        }
    };

    private static final Map<Class, Class> nativeCounterParts = new HashMap<Class, Class>() {
        {
            put(Boolean.class, boolean.class);
            put(Short.class, short.class);
            put(Long.class, long.class);
            put(Integer.class, int.class);
            put(Float.class, float.class);
            put(Double.class, double.class);
        }
    };

    private Map<String, Method> fromBytesMethods, toBytesMethods;
    private Map<String, Constructor> constructors;

    public HBObjectMapper() {
        fromBytesMethods = new HashMap<String, Method>(fromBytesMethodNames.size());
        toBytesMethods = new HashMap<String, Method>(fromBytesMethodNames.size());
        constructors = new HashMap<String, Constructor>(fromBytesMethodNames.size());
        for (Map.Entry<Class, String> e : fromBytesMethodNames.entrySet()) {
            Class<?> clazz = e.getKey();
            String toDataTypeMethodName = e.getValue();
            Method fromBytesMethod, toBytesMethod;
            Constructor<?> constructor;
            try {
                fromBytesMethod = Bytes.class.getDeclaredMethod(toDataTypeMethodName, byte[].class);
                toBytesMethod = Bytes.class.getDeclaredMethod("toBytes", nativeCounterParts.containsKey(clazz) ? nativeCounterParts.get(clazz) : clazz);
                constructor = clazz.getConstructor(String.class);
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Bug in %s code -  Contact developer", HBObjectMapper.class.getName()));
            }
            fromBytesMethods.put(clazz.getName(), fromBytesMethod);
            toBytesMethods.put(clazz.getName(), toBytesMethod);
            constructors.put(clazz.getName(), constructor);
        }
    }

    private byte[] fieldValueToByteArray(Field field, HBRecord obj, boolean serializeAsString) {
        Class<?> fieldType = field.getType();
        try {
            if (!toBytesMethods.containsKey(fieldType.getName())) {
                throw new IllegalArgumentException(String.format("Don't know how to convert field of type %s to byte array", fieldType.getName()));
            }
            Method toBytesMethod = toBytesMethods.get(fieldType.getName());
            Object fieldValue = field.get(obj);
            if (fieldValue == null) return null;
            Object fieldValueBytes = serializeAsString ? Bytes.toBytes(String.valueOf(fieldValue)) : toBytesMethod.invoke(obj, fieldValue);
            return (byte[]) fieldValueBytes;
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Bug in library code. Contact developer", e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(String.format("Error occurred while calling %s.fieldValueToByteArray(%s)", Bytes.class.getName(), fieldType.getName()), e);
        }
    }

    private NavigableMap<byte[], NavigableMap<byte[], byte[]>> objToMap(HBRecord obj) {
        Class clazz = obj.getClass();
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
        int numColumnsToWrite = 0;
        for (Field field : clazz.getDeclaredFields()) {
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            if (hbColumn == null) {
                continue;
            }
            int modifiers = field.getModifiers();
            if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers)) {
                throw new IllegalArgumentException(String.format("In class \"%s\", the field \"%s\" is annotated with \"%s\", but is declared as static/transient", clazz.getName(), field.getName(), HBColumn.class.getName()));
            }
            Class<?> fieldClazz = field.getType();
            if (!fromBytesMethods.containsKey(fieldClazz.getName())) {
                throw new IllegalArgumentException(String.format("Field %s in class %s is of unsupported type (%s). List of supported types: %s", field.getName(), clazz.getName(), fieldClazz.getName(), fromBytesMethods.keySet()));
            }
            byte[] family = Bytes.toBytes(hbColumn.family()), columnName = Bytes.toBytes(hbColumn.column());
            if (!map.containsKey(family)) {
                map.put(family, new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
            }
            Map<byte[], byte[]> columns = map.get(family);
            if (columns.containsKey(columnName)) {
                throw new IllegalArgumentException(String.format("Class %s has two fields mapped to same column %s:%s", clazz.getName(), hbColumn.family(), hbColumn.column()));
            }
            field.setAccessible(true);
            byte[] columnValue = fieldValueToByteArray(field, obj, hbColumn.serializeAsString());
            if (columnValue == null || columnValue.length == 0) {
                continue;
            }
            columns.put(columnName, columnValue);
            numColumnsToWrite++;
        }
        if (numColumnsToWrite == 0) {
            throw new IllegalArgumentException("Cannot accept input object with all it's column-mapped variables null");
        }
        return map;
    }

    /**
     * Converts a bean-like object to HBase's {@link Put} object. For use in reducer jobs that extend HBase's {@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}
     *
     * @param obj bean-like object (must extend {@link HBRecord})
     * @return HBase's {@link Put} object
     */
    public Put writeValueAsPut(HBRecord obj) {
        String rowKey = composeRowKey(obj);
        byte[] row = Bytes.toBytes(rowKey);
        Put put = new Put(row);
        for (NavigableMap.Entry<byte[], NavigableMap<byte[], byte[]>> fe : objToMap(obj).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], byte[]> e : fe.getValue().entrySet()) {
                put.add(family, e.getKey(), e.getValue());
            }
        }
        return put;
    }

    /**
     * Converts a list of bean-like objects to a list of HBase's {@link Put} objects
     *
     * @param objs List of bean-like objects (must extend {@link HBRecord})
     * @return List of HBase's {@link Put} objects
     */
    public List<Put> writeValueAsPut(List<HBRecord> objs) {
        List<Put> puts = new ArrayList<Put>(objs.size());
        for (HBRecord obj : objs) {
            Put put = writeValueAsPut(obj);
            puts.add(put);
        }
        return puts;
    }

    /**
     * Converts a bean-like object to HBase's {@link Result} object. For unit-testing mapper jobs that extend {@link org.apache.hadoop.hbase.mapreduce.TableMapper TableMapper}
     *
     * @param obj bean-like object (must extend {@link HBRecord})
     * @return HBase's {@link Result} object
     */
    public Result writeValueAsResult(HBRecord obj) {
        String rowKey = composeRowKey(obj);
        byte[] row = Bytes.toBytes(rowKey);
        List<KeyValue> keyValueList = new ArrayList<KeyValue>();
        for (NavigableMap.Entry<byte[], NavigableMap<byte[], byte[]>> fe : objToMap(obj).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], byte[]> e : fe.getValue().entrySet()) {
                keyValueList.add(new KeyValue(row, family, e.getKey(), e.getValue()));
            }
        }
        return new Result(keyValueList);
    }

    /**
     * Converts a list of bean-like objects to a list of HBase's {@link Result} objects
     *
     * @param objs List of bean-like objects (must extend {@link HBRecord})
     * @return List of HBase's {@link Result} objects
     */
    public List<Result> writeValueAsResult(List<HBRecord> objs) {
        List<Result> results = new ArrayList<Result>(objs.size());
        for (HBRecord obj : objs) {
            Result result = writeValueAsResult(obj);
            results.add(result);
        }
        return results;
    }

    /**
     * Converts HBase's {@link Result} object to a bean-like object. For use in  mapper jobs that extend {@link org.apache.hadoop.hbase.mapreduce.TableMapper TableMapper}
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz) {
        return readValueFromResult(rowKey.get(), result, clazz);
    }

    /**
     * Converts HBase's {@link Result} object to a bean-like object
     *
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(Result result, Class<T> clazz) {
        return readValueFromResult(result.getRow(), result, clazz);
    }

    /**
     * Converts HBase's {@link Result} object to a bean-like object. For use in DAO of an HBase table
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(String rowKey, Result result, Class<T> clazz) {
        return readValueFromResult(Bytes.toBytes(rowKey), result, clazz);
    }

    private <T extends HBRecord> T readValueFromResult(byte[] rowKey, Result result, Class<T> clazz) {
        if (result == null) {
            throw new IllegalArgumentException("Result object cannot be null");
        }
        if (result.isEmpty()) {
            return null;
        }
        return mapToObj(rowKey, result.getNoVersionMap(), clazz);
    }

    private <T extends HBRecord> T mapToObj(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], byte[]>> map, Class<T> clazz) {
        if (rowKeyBytes == null) {
            throw new IllegalArgumentException("Row key cannot be null");
        } else if (map == null) {
            throw new IllegalStateException("Could not convert from Result/Put object");
        }
        String rowKey = Bytes.toString(rowKeyBytes);
        T obj;
        try {
            try {
                obj = clazz.newInstance();
            } catch (InstantiationException iex) {
                throw new IllegalArgumentException(String.format("Class %s should specify an empty constructor", clazz.getName()));
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(String.format("Cannot instantiate empty constructor of %s", clazz.getName()));
            }

            try {
                obj.parseRowKey(rowKey);
            } catch (Exception ex) {
                throw new IllegalArgumentException(String.format("Supplied row key \"%s\" could not be parsed", rowKey), ex);
            }
            for (Field field : clazz.getDeclaredFields()) {
                HBColumn hbColumn = field.getAnnotation(HBColumn.class);
                if (hbColumn == null)
                    continue;
                field.setAccessible(true);
                NavigableMap<byte[], byte[]> familyMap = map.get(Bytes.toBytes(hbColumn.family()));
                if (familyMap == null || familyMap.size() == 0)
                    continue;
                byte[] value = familyMap.get(Bytes.toBytes(hbColumn.column()));
                if (value == null || value.length == 0)
                    continue;
                Class fieldClazz = field.getType();
                if (hbColumn.serializeAsString()) {
                    Constructor constructor = constructors.get(fieldClazz.getName());
                    Object fieldValue;
                    try {
                        fieldValue = constructor.newInstance(Bytes.toString(value));
                    } catch (Exception nfex) {
                        fieldValue = null;
                    }
                    field.set(obj, fieldValue);
                } else {
                    Method method = fromBytesMethods.get(fieldClazz.getName());
                    field.set(obj, method.invoke(null, new Object[]{value}));
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Could not map object for row key \"%s\"", rowKey), e);
        }
        return obj;
    }

    /**
     * Converts HBase's {@link Put} object to a bean-like object. For unit-testing reducer jobs that extend {@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}
     *
     * @param rowKeyBytes Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put         HBase's {@link Put} object
     * @param clazz       {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(ImmutableBytesWritable rowKeyBytes, Put put, Class<T> clazz) {
        return readValueFromPut(rowKeyBytes == null ? put.getRow() : rowKeyBytes.get(), put, clazz);
    }


    /**
     * Converts HBase's {@link Put} object to a bean-like object
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(String rowKey, Put put, Class<T> clazz) {
        return readValueFromPut(Bytes.toBytes(rowKey), put, clazz);
    }

    private <T extends HBRecord> T readValueFromPut(byte[] rowKey, Put put, Class<T> clazz) {
        Map<byte[], List<KeyValue>> rawMap = put.getFamilyMap();
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], List<KeyValue>> e : rawMap.entrySet()) {
            for (KeyValue kv : e.getValue()) {
                if (!map.containsKey(kv.getFamily())) {
                    map.put(kv.getFamily(), new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], byte[]> columnValues = map.get(kv.getFamily());
                columnValues.put(kv.getQualifier(), kv.getValue());
            }
        }
        return mapToObj(rowKey, map, clazz);
    }

    /**
     * Converts HBase's {@link Put} object to a bean-like object. Use-cases: Reducer test cases
     *
     * @param put   HBase's {@link Put} object
     * @param clazz {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(Put put, Class<T> clazz) {
        return readValueFromPut(put.getRow(), put, clazz);
    }

    /**
     * Get row key (for use in HBase) from a bean-line object
     *
     * @param obj Bean-line object (must extend {@link HBRecord})
     * @return Row key
     */
    public ImmutableBytesWritable getRowKey(HBRecord obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Cannot accept null objects");
        }
        String rowKey = composeRowKey(obj);
        return Util.strToIbw(rowKey);
    }

    private String composeRowKey(HBRecord obj) {
        String rowKey;
        try {
            rowKey = obj.composeRowKey();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Error while composing row key for object " + obj);
        }
        return rowKey;
    }

}