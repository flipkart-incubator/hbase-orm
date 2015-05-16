package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.exceptions.*;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

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

    private static final BiMap<Class, Class> nativeCounterParts = HashBiMap.create(new HashMap<Class, Class>() {
        {
            put(Boolean.class, boolean.class);
            put(Short.class, short.class);
            put(Long.class, long.class);
            put(Integer.class, int.class);
            put(Float.class, float.class);
            put(Double.class, double.class);
        }
    });

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
                throw new BadHBaseLibStateException(ex);
            }
            fromBytesMethods.put(clazz.getName(), fromBytesMethod);
            toBytesMethods.put(clazz.getName(), toBytesMethod);
            constructors.put(clazz.getName(), constructor);
        }
    }

    private <T extends HBRecord> T mapToObj(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], byte[]>> map, Class<T> clazz) {
        String rowKey = Bytes.toString(rowKeyBytes);
        T obj;
        validateHBClass(clazz);
        try {
            obj = clazz.newInstance();
        } catch (Exception ex) {
            throw new ObjectNotInstantiatableException("Error while instantiating empty constructor of " + clazz.getName(), ex);
        }
        try {
            obj.parseRowKey(rowKey);
        } catch (Exception ex) {
            throw new RowKeyCouldNotBeParsedException(String.format("Supplied row key \"%s\" could not be parsed", rowKey), ex);
        }
        for (Field field : clazz.getDeclaredFields()) {
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            if (hbColumn == null)
                continue;
            NavigableMap<byte[], byte[]> familyMap = map.get(Bytes.toBytes(hbColumn.family()));
            if (familyMap == null || familyMap.isEmpty())
                continue;
            byte[] value = familyMap.get(Bytes.toBytes(hbColumn.column()));
            objectSetFieldValue(obj, field, value);
        }
        return obj;
    }

    private boolean isFieldNull(Field field, HBRecord obj) {
        try {
            field.setAccessible(true);
            return field.get(obj) == null;
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException("Field " + field.getName() + " could not be accessed", e);
        }
    }

    private byte[] getFieldValueAsByteArray(Field field, HBRecord obj, boolean serializeAsString) {
        field.setAccessible(true);
        Class<?> fieldType = field.getType();
        try {
            if (!toBytesMethods.containsKey(fieldType.getName())) {
                throw new ConversionFailedException(String.format("Don't know how to convert field of type %s to byte array", fieldType.getName()));
            }
            Method toBytesMethod = toBytesMethods.get(fieldType.getName());
            Object fieldValue = field.get(obj);
            if (fieldValue == null)
                return null;
            Object fieldValueBytes = serializeAsString ? Bytes.toBytes(String.valueOf(fieldValue)) : toBytesMethod.invoke(obj, fieldValue);
            return (byte[]) fieldValueBytes;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        } catch (InvocationTargetException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    private <T extends HBRecord> void validateHBClass(Class<T> clazz) {
        Constructor constructor;
        try {
            Set<Pair<String, String>> columns = new HashSet<Pair<String, String>>();
            constructor = clazz.getDeclaredConstructor();
            int numOfHBColumns = 0, numOfHBRowKeys = 0;
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(HBRowKey.class)) {
                    numOfHBRowKeys++;
                }
                HBColumn hbColumn = field.getAnnotation(HBColumn.class);
                if (hbColumn == null)
                    continue;
                validateHBColumnField(clazz, field);
                numOfHBColumns++;
                if (!columns.add(new Pair<String, String>(hbColumn.family(), hbColumn.column()))) {
                    throw new FieldsMappedToSameColumnException(String.format("Class %s has two fields mapped to same column %s:%s", clazz.getName(), hbColumn.family(), hbColumn.column()));
                }
            }
            if (numOfHBColumns == 0) {
                throw new MissingHBColumnFieldsException(String.format("Class %s doesn't even have a single field annotated with %s", clazz.getName(), HBColumn.class.getName()));
            }
            if (numOfHBRowKeys == 0) {
                throw new MissingHBRowKeyFieldsException(String.format("Class %s doesn't even have a single field annotated with %s (how else would you construct the row key for HBase record?)", clazz.getName(), HBRowKey.class.getName()));
            }

        } catch (NoSuchMethodException e) {
            throw new NoEmptyConstructorException(String.format("Class %s needs to specify an empty constructor", clazz.getName()), e);
        }
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new EmptyConstructorInaccessibleException(String.format("Empty constructor of class %s is inaccessible", clazz.getName()));
        }

    }

    private <T extends HBRecord> void validateHBColumnField(Class<T> clazz, Field field) {
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(String.format("In class \"%s\", the field \"%s\" is annotated with \"%s\", but is declared as transient (Transient fields cannot be persisted)", clazz.getName(), field.getName(), HBColumn.class.getName()));
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(String.format("In class \"%s\", the field \"%s\" is annotated with \"%s\", but is declared as static (Only instance fields can be mapped to HBase columns)", clazz.getName(), field.getName(), HBColumn.class.getName()));
        }
        Class<?> fieldClazz = field.getType();
        if (fieldClazz.isPrimitive()) {
            String suggestion = nativeCounterParts.containsValue(fieldClazz) ? String.format("- Use type %s instead", nativeCounterParts.inverse().get(fieldClazz).getName()) : "";
            throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable) %s", field.getName(), clazz.getName(), fieldClazz.getName(), suggestion));
        }
        if (!fromBytesMethods.containsKey(fieldClazz.getName())) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s). List of supported types: %s", field.getName(), clazz.getName(), fieldClazz.getName(), fromBytesMethods.keySet()));
        }
    }

    private NavigableMap<byte[], NavigableMap<byte[], byte[]>> objToMap(HBRecord obj) {
        Class<? extends HBRecord> clazz = obj.getClass();
        validateHBClass(clazz);
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        for (Field field : clazz.getDeclaredFields()) {
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            boolean isRowKey = field.isAnnotationPresent(HBRowKey.class);
            if (hbColumn == null && !isRowKey)
                continue;
            if (isRowKey) {
                if (isFieldNull(field, obj))
                    throw new HBRowKeyFieldCantBeNullException("Field " + field.getName() + " is null (fields part of row key cannot be null)");
            }
            if (hbColumn != null) {
                byte[] family = Bytes.toBytes(hbColumn.family()), columnName = Bytes.toBytes(hbColumn.column());
                if (!map.containsKey(family)) {
                    map.put(family, new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], byte[]> columns = map.get(family);
                byte[] fieldValue = getFieldValueAsByteArray(field, obj, hbColumn.serializeAsString());
                boolean isFieldNull = (fieldValue == null || fieldValue.length == 0);
                if (isFieldNull) {
                    continue;
                }
                numOfFieldsToWrite++;
                columns.put(columnName, fieldValue);
            }
        }
        if (numOfFieldsToWrite == 0) {
            throw new AllHBColumnFieldsNullException("Cannot accept input object with all it's column-mapped variables null");
        }
        return map;
    }

    /**
     * Converts a bean-like object to HBase's {@link Put} object. For use in reducer jobs that extend HBase's {@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}
     *
     * @param obj bean-like object (of type that extends {@link HBRecord})
     * @return HBase's {@link Put} object
     */
    public Put writeValueAsPut(HBRecord obj) {
        Put put = new Put(composeRowKey(obj));
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
     * @param objs List of bean-like objects (of type that extends {@link HBRecord})
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
     * Converts a bean-like object to HBase's {@link Result} object. For use in unit-tests of mapper jobs that extend {@link org.apache.hadoop.hbase.mapreduce.TableMapper TableMapper}
     *
     * @param obj bean-like object (of type that extends {@link HBRecord})
     * @return HBase's {@link Result} object
     */
    public Result writeValueAsResult(HBRecord obj) {
        byte[] row = composeRowKey(obj);
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
     * @param objs List of bean-like objects (of type that extends {@link HBRecord})
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
        if (rowKey == null)
            return readValueFromResult(result, clazz);
        else
            return readValueFromRowAndResult(rowKey.get(), result, clazz);
    }

    /**
     * Converts HBase's {@link Result} object to a bean-like object
     *
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(Result result, Class<T> clazz) {
        return readValueFromResult(result, clazz);
    }

    /**
     * Converts HBase's {@link Result} object to a bean-like object. For building data access objects for an HBase table
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(String rowKey, Result result, Class<T> clazz) {
        if (rowKey == null)
            return readValueFromResult(result, clazz);
        else
            return readValueFromRowAndResult(Bytes.toBytes(rowKey), result, clazz);
    }

    private boolean isResultEmpty(Result result) {
        return result == null || result.isEmpty() || result.getRow() == null || result.getRow().length == 0;
    }

    private <T extends HBRecord> T readValueFromResult(Result result, Class<T> clazz) {
        if (isResultEmpty(result)) return null;
        return mapToObj(result.getRow(), result.getNoVersionMap(), clazz);
    }

    private <T extends HBRecord> T readValueFromRowAndResult(byte[] rowKey, Result result, Class<T> clazz) {
        if (isResultEmpty(result)) return null;
        return mapToObj(rowKey, result.getNoVersionMap(), clazz);
    }

    private void objectSetFieldValue(Object obj, Field field, byte[] value) {
        if (value == null || value.length == 0)
            return;
        try {
            field.setAccessible(true);
            field.set(obj, toFieldValue(value, field));
        } catch (Exception ex) {
            throw new ConversionFailedException("Could not set value on field \"" + field.getName() + "\" on instance of class " + obj.getClass(), ex);
        }
    }

    public Object toFieldValue(byte[] value, Field field) {
        if (value == null)
            return null;
        Object fieldValue;
        try {
            Class<?> fieldClazz = field.getType();
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            if (hbColumn.serializeAsString()) {
                Constructor constructor = constructors.get(fieldClazz.getName());
                try {
                    fieldValue = constructor.newInstance(Bytes.toString(value));
                } catch (Exception ex) {
                    fieldValue = null;
                }
            } else {
                Method method = fromBytesMethods.get(fieldClazz.getName());
                fieldValue = method.invoke(null, new Object[]{value});
            }
            return fieldValue;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        } catch (InvocationTargetException e) {
            throw new BadHBaseLibStateException(e);
        }
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
        if (rowKeyBytes == null)
            return readValueFromPut(put, clazz);
        else
            return readValueFromRowAndPut(rowKeyBytes.get(), put, clazz);
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
        if (rowKey == null)
            return readValueFromPut(put, clazz);
        else
            return readValueFromRowAndPut(Bytes.toBytes(rowKey), put, clazz);
    }

    private <T extends HBRecord> T readValueFromRowAndPut(byte[] rowKey, Put put, Class<T> clazz) {
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

    private <T extends HBRecord> T readValueFromPut(Put put, Class<T> clazz) {
        if (put == null || put.isEmpty() || put.getRow() == null || put.getRow().length == 0) {
            return null;
        }
        return readValueFromRowAndPut(put.getRow(), put, clazz);
    }

    /**
     * Converts HBase's {@link Put} object to a bean-like object
     *
     * @param put   HBase's {@link Put} object
     * @param clazz {@link Class} to which you want to convert to (must extend {@link HBRecord} class)
     * @return Bean-like object
     */
    public <T extends HBRecord> T readValue(Put put, Class<T> clazz) {
        return readValueFromPut(put, clazz);
    }

    /**
     * Get row key (for use in HBase) from a bean-line object.<br>
     * For use in:
     * <ul>
     * <li>reducer jobs that extend HBase's {@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}</li>
     * <li>unit tests for mapper jobs that extend HBase's {@link org.apache.hadoop.hbase.mapreduce.TableMapper TableMapper}</li>
     * </ul>
     *
     * @param obj bean-like object (of type that extends {@link HBRecord})
     * @return Row key
     */
    public ImmutableBytesWritable getRowKey(HBRecord obj) {
        if (obj == null) {
            throw new NullPointerException("Cannot compose row key for null objects");
        }
        return new ImmutableBytesWritable(composeRowKey(obj));
    }

    private byte[] composeRowKey(HBRecord obj) {
        String rowKey;
        try {
            rowKey = obj.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException("Error while composing row key for object", ex);
        }
        if (rowKey == null || rowKey.isEmpty()) {
            throw new RowKeyCantBeEmptyException("Row key composed for object is null or empty");
        }
        return Bytes.toBytes(rowKey);
    }

    /**
     * Get list of column families mapped in definition of your bean-like class
     *
     * @param clazz {@link Class} that you're reading (must extend {@link HBRecord} class)
     * @return Return set of column families used in input class
     */
    public <T extends HBRecord> Set<String> getColumnFamilies(Class<T> clazz) {
        validateHBClass(clazz);
        Set<String> columnFamilySet = new HashSet<String>();
        for (Field field : clazz.getDeclaredFields()) {
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            if (hbColumn == null) continue;
            columnFamilySet.add(hbColumn.family());
        }
        return columnFamilySet;
    }

    /**
     * Converts a bean-like object to a {@link Pair} of row key (of type {@link ImmutableBytesWritable}) and HBase's {@link Result} object
     *
     * @param obj bean-like object (of type that extends {@link HBRecord})
     */
    public Pair<ImmutableBytesWritable, Result> writeValueAsRowKeyResultPair(HBRecord obj) {
        return new Pair<ImmutableBytesWritable, Result>(this.getRowKey(obj), this.writeValueAsResult(obj));
    }

    /**
     * Converts a list of bean-like objects to a {@link Pair}s of row keys (of type {@link ImmutableBytesWritable}) and HBase's {@link Result} objects
     *
     * @param objs List of bean-like objects (of type that extends {@link HBRecord})
     */
    public List<Pair<ImmutableBytesWritable, Result>> writeValueAsRowKeyResultPair(List<? extends HBRecord> objs) {
        List<Pair<ImmutableBytesWritable, Result>> pairList = new ArrayList<Pair<ImmutableBytesWritable, Result>>(objs.size());
        for (HBRecord obj : objs) {
            pairList.add(writeValueAsRowKeyResultPair(obj));
        }
        return pairList;
    }

    /**
     * Checks whether input class can be converted to HBase data types and vice-versa
     *
     * @param clazz {@link Class} you intend to validate (must extend {@link HBRecord} class)
     */
    public <T extends HBRecord> boolean isValid(Class<T> clazz) {
        try {
            validateHBClass(clazz);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Get field definitions HBase column mapped fields for your {@link Class}
     *
     * @param clazz Bean-like {@link Class} (must extend {@link HBRecord} class) whose fields you intend to read
     */
    public <T extends HBRecord> Map<String, Field> getHBFields(Class<T> clazz) {
        validateHBClass(clazz);
        Map<String, Field> mappings = new HashMap<String, Field>();
        for (Field field : clazz.getDeclaredFields()) {
            HBColumn hbColumn = field.getAnnotation(HBColumn.class);
            if (hbColumn == null)
                continue;
            mappings.put(field.getName(), field);
        }
        return mappings;
    }
}