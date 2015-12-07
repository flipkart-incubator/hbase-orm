package com.flipkart.hbaseobjectmapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * An object mapper class that helps convert your bean-like objects to HBase's {@link Put} and {@link Result} objects (and vice-versa). For use in Map/Reduce jobs and their unit-tests
 */
public class HBObjectMapper {

    private static final ObjectMapper jsonObjMapper = new ObjectMapper();

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

    private final Map<String, Method> fromBytesMethods, toBytesMethods;
    private final Map<String, Constructor> constructors;

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

    private <T extends HBRecord> T mapToObj(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> clazz) {
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
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            if (hbColumn.isSingleVersioned()) {
                NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(Bytes.toBytes(hbColumn.family()));
                if (familyMap == null || familyMap.isEmpty())
                    continue;
                NavigableMap<Long, byte[]> columnVersionsMap = familyMap.get(Bytes.toBytes(hbColumn.column()));
                if (columnVersionsMap == null || columnVersionsMap.isEmpty())
                    continue;
                Map.Entry<Long, byte[]> lastEntry = columnVersionsMap.lastEntry();
                objectSetFieldValue(obj, field, lastEntry.getValue(), hbColumn.serializeAsString());
            } else if (hbColumn.isMultiVersioned()) {
                NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(Bytes.toBytes(hbColumn.family()));
                if (familyMap == null || familyMap.isEmpty())
                    continue;
                NavigableMap<Long, byte[]> columnVersionsMap = familyMap.get(Bytes.toBytes(hbColumn.column()));
                objectSetFieldValue(obj, field, columnVersionsMap, hbColumn.serializeAsString());
            }
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

    private byte[] valueToByteArray(Class<?> clazz, Object value, boolean serializeAsString) {
        try {
            if (value == null)
                return null;
            if (toBytesMethods.containsKey(clazz.getName())) {
                Method toBytesMethod = toBytesMethods.get(clazz.getName());
                return serializeAsString ? Bytes.toBytes(String.valueOf(value)) : (byte[]) toBytesMethod.invoke(null, value);
            } else {
                try {
                    if (serializeAsString)
                        return Bytes.toBytes(jsonObjMapper.writeValueAsString(value));
                    else
                        return jsonObjMapper.writeValueAsBytes(value);

                } catch (JsonProcessingException jpx) {
                    throw new ConversionFailedException(String.format("Don't know how to convert field of type %s to byte array", clazz.getName()));
                }
            }
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        } catch (InvocationTargetException e) {
            throw new BadHBaseLibStateException(e);
        } catch (IllegalArgumentException iax) {
            throw new BadHBaseLibStateException(iax);
        }
    }

    private static <T extends HBRecord> void validateHBClass(Class<T> clazz) {
        Constructor constructor;
        try {
            Set<Pair<String, String>> columns = new HashSet<Pair<String, String>>();
            constructor = clazz.getDeclaredConstructor();
            int numOfHBColumns = 0, numOfHBRowKeys = 0;
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(HBRowKey.class)) {
                    numOfHBRowKeys++;
                }
                WrappedHBColumn hbColumn = new WrappedHBColumn(field);
                if (hbColumn.isSingleVersioned()) {
                    validateHBColumnField(field);
                    numOfHBColumns++;
                    if (!columns.add(new Pair<String, String>(hbColumn.family(), hbColumn.column()))) {
                        throw new FieldsMappedToSameColumnException(String.format("Class %s has two fields mapped to same column %s:%s", clazz.getName(), hbColumn.family(), hbColumn.column()));
                    }
                } else if (hbColumn.isMultiVersioned()) {
                    validateHBColumnMultiVersionField(field);
                    numOfHBColumns++;
                    if (!columns.add(new Pair<String, String>(hbColumn.family(), hbColumn.column()))) {
                        throw new FieldsMappedToSameColumnException(String.format("Class %s has two fields mapped to same column %s:%s", clazz.getName(), hbColumn.family(), hbColumn.column()));
                    }
                }
            }
            if (numOfHBColumns == 0) {
                throw new MissingHBColumnFieldsException(clazz);
            }
            if (numOfHBRowKeys == 0) {
                throw new MissingHBRowKeyFieldsException(clazz);
            }

        } catch (NoSuchMethodException e) {
            throw new NoEmptyConstructorException(String.format("Class %s needs to specify an empty constructor", clazz.getName()), e);
        }
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new EmptyConstructorInaccessibleException(String.format("Empty constructor of class %s is inaccessible", clazz.getName()));
        }
    }

    private static void validateHBColumnMultiVersionField(Field field) {
        validateHBColumnField(field);
        if (!(field.getGenericType() instanceof ParameterizedType)) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " is not even a parameterized type");
        }
        if (field.getType() != NavigableMap.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " is not a NavigableMap");
        }
        ParameterizedType pType = (ParameterizedType) field.getGenericType();
        Type[] typeArguments = pType.getActualTypeArguments();
        if (typeArguments.length != 2 || typeArguments[0] != Long.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " has unexpected type params");
        }
    }

    private static <T extends HBRecord> void validateHBColumnField(Field field) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) field.getDeclaringClass();
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(field, hbColumn.getName());
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(field, hbColumn.getName());
        }
        Class<?> fieldClazz = field.getType();
        if (fieldClazz.isPrimitive()) {
            String suggestion = nativeCounterParts.containsValue(fieldClazz) ? String.format("- Use type %s instead", nativeCounterParts.inverse().get(fieldClazz).getName()) : "";
            throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable) %s", field.getName(), clazz.getName(), fieldClazz.getName(), suggestion));
        }
        JavaType javaType = jsonObjMapper.constructType(field.getGenericType());
        if (!jsonObjMapper.canDeserialize(javaType)) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s)", field.getName(), clazz.getName(), fieldClazz.getName()));
        }
    }

    private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> objToMap(HBRecord obj) {
        Class<? extends HBRecord> clazz = obj.getClass();
        validateHBClass(clazz);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        for (Field field : clazz.getDeclaredFields()) {
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            boolean isRowKey = field.isAnnotationPresent(HBRowKey.class);
            if (!hbColumn.isPresent() && !isRowKey)
                continue;
            if (isRowKey && isFieldNull(field, obj)) {
                throw new HBRowKeyFieldCantBeNullException("Field " + field.getName() + " is null (fields part of row key cannot be null)");
            }
            if (hbColumn.isSingleVersioned()) {
                byte[] family = Bytes.toBytes(hbColumn.family()), columnName = Bytes.toBytes(hbColumn.column());
                if (!map.containsKey(family)) {
                    map.put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(family);
                final byte[] fieldValueBytes = getFieldValueAsBytes(obj, field, hbColumn.serializeAsString());
                if (fieldValueBytes == null || fieldValueBytes.length == 0) {
                    continue;
                }
                numOfFieldsToWrite++;
                columns.put(columnName, new TreeMap<Long, byte[]>() {
                    {
                        put(HConstants.LATEST_TIMESTAMP, fieldValueBytes);
                    }
                });
            } else if (hbColumn.isMultiVersioned()) {
                NavigableMap<Long, byte[]> fieldValueVersions = getFieldValuesVersioned(field, obj, hbColumn.serializeAsString());
                if (fieldValueVersions == null)
                    continue;
                byte[] family = Bytes.toBytes(hbColumn.family()), columnName = Bytes.toBytes(hbColumn.column());
                if (!map.containsKey(family)) {
                    map.put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(family);
                numOfFieldsToWrite++;
                columns.put(columnName, fieldValueVersions);
            }
        }
        if (numOfFieldsToWrite == 0) {
            throw new AllHBColumnFieldsNullException();
        }
        return map;
    }

    private byte[] getFieldValueAsBytes(HBRecord obj, Field field, boolean serializeAsString) {
        Object fieldValue;
        try {
            field.setAccessible(true);
            fieldValue = field.get(obj);
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
        return valueToByteArray(field.getType(), fieldValue, serializeAsString);
    }

    private NavigableMap<Long, byte[]> getFieldValuesVersioned(Field field, HBRecord obj, boolean serializeAsString) {
        ParameterizedType fieldNavigableMapType = ((ParameterizedType) field.getGenericType());
        Class<?> fieldType = jsonObjMapper.constructType(fieldNavigableMapType).getContentType().getRawClass();
        try {
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            NavigableMap<Long, Object> fieldValueVersions = (NavigableMap<Long, Object>) field.get(obj);
            if (fieldValueVersions == null)
                return null;
            if (fieldValueVersions.size() == 0) {
                throw new FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty();
            }
            NavigableMap<Long, byte[]> output = new TreeMap<Long, byte[]>();
            for (NavigableMap.Entry<Long, Object> e : fieldValueVersions.entrySet()) {
                Long timestamp = e.getKey();
                Object fieldValue = e.getValue();
                if (fieldValue == null)
                    continue;
                byte[] fieldValueBytes = valueToByteArray(fieldType, fieldValue, serializeAsString);
                output.put(timestamp, fieldValueBytes);
            }
            return output;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    /**
     * Converts a bean-like object to HBase's {@link Put} object. For use in reducer jobs that extend HBase's {@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}
     *
     * @param obj bean-like object (of type that extends {@link HBRecord})
     * @return HBase's {@link Put} object
     */
    public Put writeValueAsPut(HBRecord obj) {
        Put put = new Put(composeRowKey(obj));
        for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : objToMap(obj).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> columnValuesVersioned = e.getValue();
                if (columnValuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                    put.add(family, columnName, versionAndValue.getKey(), versionAndValue.getValue());
                }
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
    public List<Put> writeValueAsPut(List<? extends HBRecord> objs) {
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
        for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : objToMap(obj).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> columnVersion : valuesVersioned.entrySet()) {
                    keyValueList.add(new KeyValue(row, family, columnName, columnVersion.getKey(), columnVersion.getValue()));
                }
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
    public List<Result> writeValueAsResult(List<? extends HBRecord> objs) {
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
        return mapToObj(result.getRow(), result.getMap(), clazz);
    }

    private <T extends HBRecord> T readValueFromRowAndResult(byte[] rowKey, Result result, Class<T> clazz) {
        if (isResultEmpty(result)) return null;
        return mapToObj(rowKey, result.getMap(), clazz);
    }

    private void objectSetFieldValue(Object obj, Field field, NavigableMap<Long, byte[]> columnValuesVersioned, boolean serializeAsString) {
        if (columnValuesVersioned == null)
            return;
        try {
            field.setAccessible(true);
            NavigableMap<Long, Object> columnValuesVersionedBoxed = new TreeMap<Long, Object>();
            Class<?> fieldType = jsonObjMapper.constructType(field.getGenericType()).getContentType().getRawClass();
            for (NavigableMap.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                columnValuesVersionedBoxed.put(versionAndValue.getKey(), byteArrayToValue(versionAndValue.getValue(), fieldType, serializeAsString));
            }
            field.set(obj, columnValuesVersionedBoxed);
        } catch (Exception ex) {
            throw new ConversionFailedException("Could not set value on field \"" + field.getName() + "\" on instance of class " + obj.getClass(), ex);
        }
    }

    private void objectSetFieldValue(Object obj, Field field, byte[] value, boolean serializeAsString) {
        if (value == null || value.length == 0)
            return;
        try {
            field.setAccessible(true);
            field.set(obj, byteArrayToValue(value, field.getType(), serializeAsString));
        } catch (Exception ex) {
            throw new ConversionFailedException("Could not set value on field \"" + field.getName() + "\" on instance of class " + obj.getClass(), ex);
        }
    }


    /**
     * Convert a byte array representing HBase column data to appropriate data type (boxed as object)
     */
    Object byteArrayToValue(byte[] value, Class<?> fieldClazz, boolean serializeAsString) {
        if (value == null || value.length == 0)
            return null;
        Object fieldValue;
        try {
            if (fromBytesMethods.containsKey(fieldClazz.getName())) {
                if (serializeAsString) {
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
            } else {
                try {
                    JavaType fieldType = jsonObjMapper.constructType(fieldClazz);
                    if (serializeAsString)
                        return jsonObjMapper.readValue(Bytes.toString(value), fieldType);
                    else
                        return jsonObjMapper.readValue(value, fieldType);
                } catch (IOException e) {
                    throw new CouldNotDeserializeException(e);
                }
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
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], List<KeyValue>> familyNameAndColumnValues : rawMap.entrySet()) {
            byte[] family = familyNameAndColumnValues.getKey();
            if (!map.containsKey(family)) {
                map.put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
            }
            List<KeyValue> kvList = familyNameAndColumnValues.getValue();
            for (KeyValue kv : kvList) {
                byte[] column = kv.getQualifier();
                if (!map.get(family).containsKey(column)) {
                    map.get(family).put(column, new TreeMap<Long, byte[]>());
                }
                map.get(family).get(column).put(kv.getTimestamp(), kv.getValue());
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

    private static byte[] composeRowKey(HBRecord obj) {
        String rowKey;
        try {
            rowKey = obj.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.isEmpty()) {
            throw new RowKeyCantBeEmptyException();
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
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            if (hbColumn.isPresent())
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
        return new Pair<ImmutableBytesWritable, Result>(getRowKey(obj), this.writeValueAsResult(obj));
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
            if (new WrappedHBColumn(field).isPresent())
                mappings.put(field.getName(), field);
        }
        return mappings;
    }
}