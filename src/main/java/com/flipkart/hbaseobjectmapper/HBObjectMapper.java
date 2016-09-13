package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.JacksonJsonCodec;
import com.flipkart.hbaseobjectmapper.codec.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import com.flipkart.hbaseobjectmapper.exceptions.InternalError;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Serializable;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * <p>An object mapper class that helps convert your objects of your bean-like class to HBase's {@link Put} and {@link Result} objects (and vice-versa).</p>
 * <p>This class is for use in MapReduce jobs which read from and/or write to HBase tables and their unit-tests.</p>
 * <p>This class is thread-safe.</p>
 */
public class HBObjectMapper {

    private static final Codec DEFAULT_CODEC = new JacksonJsonCodec();

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

    private static final Map<Class, Method> fromBytesMethods, toBytesMethods;
    private static final Map<Class, Constructor> constructors;

    static {
        try {
            fromBytesMethods = new HashMap<>(fromBytesMethodNames.size());
            toBytesMethods = new HashMap<>(fromBytesMethodNames.size());
            constructors = new HashMap<>(fromBytesMethodNames.size());
            Method fromBytesMethod, toBytesMethod;
            Constructor<?> constructor;
            for (Map.Entry<Class, String> e : fromBytesMethodNames.entrySet()) {
                Class<?> clazz = e.getKey();
                String toDataTypeMethodName = e.getValue();
                fromBytesMethod = Bytes.class.getDeclaredMethod(toDataTypeMethodName, byte[].class);
                toBytesMethod = Bytes.class.getDeclaredMethod("toBytes", nativeCounterParts.containsKey(clazz) ? nativeCounterParts.get(clazz) : clazz);
                constructor = clazz.getConstructor(String.class);
                fromBytesMethods.put(clazz, fromBytesMethod);
                toBytesMethods.put(clazz, toBytesMethod);
                constructors.put(clazz, constructor);
            }
        } catch (Exception ex) {
            throw new BadHBaseLibStateException(ex);
        }
    }

    private final Codec codec;

    /**
     * Instantiate object of this class with a custom {@link Codec}
     * <p>
     * <b>Note</b>: For following java types, HBase's native serializers/deserializers are used, irrespective of what codec you specify: <ul>
     * <li>{@link Boolean}</li>
     * <li>{@link Short}</li>
     * <li>{@link Integer}</li>
     * <li>{@link Long}</li>
     * <li>{@link Float}</li>
     * <li>{@link Double}</li>
     * <li>{@link String}</li>
     * <li>{@link BigDecimal}</li>
     * </ul>
     *
     * @param codec Codec to be used for serialization and deserialization of fields
     */
    public HBObjectMapper(Codec codec) {
        this.codec = codec;
    }

    /**
     * Instantiate an object of this class with default {@link Codec} of {@link JacksonJsonCodec}
     */
    public HBObjectMapper() {
        this(DEFAULT_CODEC);
    }

    <R extends Serializable & Comparable<R>> byte[] rowKeyToBytes(R rowKey) {
        return valueToByteArray(rowKey, false);
    }

    @SuppressWarnings("unchecked")
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> R bytesToRowKey(byte[] rowKeyBytes, Class<T> entityClass) throws DeserializationException {
        try {
            return (R) byteArrayToValue(rowKeyBytes, entityClass.getDeclaredMethod("composeRowKey").getReturnType(), false);
        } catch (NoSuchMethodException e) {
            throw new InternalError(e);
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T mapToObj(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> clazz) throws DeserializationException {
        R rowKey = bytesToRowKey(rowKeyBytes, clazz);
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

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean isFieldNull(Field field, HBRecord<R> obj) {
        try {
            field.setAccessible(true);
            return field.get(obj) == null;
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException("Field " + field.getName() + " could not be accessed", e);
        }
    }

    /**
     * Converts a {@link Serializable} object into a <code>byte[]</code>
     *
     * @param value             Object to be serialized
     * @param serializeAsString If this is set to <code>true</code>, a value like <code>1.025</code>
     * @return Byte-array representing serialized object
     */
    public byte[] valueToByteArray(Serializable value, boolean serializeAsString) {
        try {
            if (value == null)
                return null;
            Class<? extends Serializable> clazz = value.getClass();
            if (toBytesMethods.containsKey(clazz)) {
                Method toBytesMethod = toBytesMethods.get(clazz);
                return serializeAsString ? Bytes.toBytes(String.valueOf(value)) : (byte[]) toBytesMethod.invoke(null, value);
            } else {
                try {
                    return codec.serialize(value);
                } catch (SerializationException jpx) {
                    throw new ConversionFailedException(String.format("Don't know how to convert field of type %s to byte array", clazz.getName()));
                }
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    public byte[] valueToByteArray(Serializable value) {
        return valueToByteArray(value, false);
    }

    /**
     * <p>Converts a {@link Serializable} object into HBase's {@link ImmutableBytesWritable}.</p>
     * <p>This method is for use in mappers and their unit-tests.</p>
     *
     * @param value Object to be serialized
     * @return Byte array, wrapped in HBase's data type
     */
    public ImmutableBytesWritable rowKeyToIbw(Serializable value) {
        return new ImmutableBytesWritable(valueToByteArray(value));
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void validateHBClass(Class<T> clazz) {
        Constructor constructor;
        try {
            Set<Pair<String, String>> columns = new HashSet<>();
            constructor = clazz.getDeclaredConstructor();
            int numOfHBColumns = 0, numOfHBRowKeys = 0;
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(HBRowKey.class)) {
                    numOfHBRowKeys++;
                }
                WrappedHBColumn hbColumn = new WrappedHBColumn(field);
                if (hbColumn.isSingleVersioned()) {
                    validateHBColumnSingleVersionField(field);
                    numOfHBColumns++;
                    if (!columns.add(new Pair<>(hbColumn.family(), hbColumn.column()))) {
                        throw new FieldsMappedToSameColumnException(String.format("Class %s has two fields mapped to same column %s:%s", clazz.getName(), hbColumn.family(), hbColumn.column()));
                    }
                } else if (hbColumn.isMultiVersioned()) {
                    validateHBColumnMultiVersionField(field);
                    numOfHBColumns++;
                    if (!columns.add(new Pair<>(hbColumn.family(), hbColumn.column()))) {
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

    /**
     * Internal note: This should be in sync with {@link #getFieldType(Field, boolean)}
     */
    private void validateHBColumnMultiVersionField(Field field) {
        validationHBColumnField(field);
        if (!(field.getGenericType() instanceof ParameterizedType)) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " is not even a parameterized type");
        }
        if (field.getType() != NavigableMap.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " is not a NavigableMap");
        }
        ParameterizedType pType = (ParameterizedType) field.getGenericType();
        Type[] typeArguments = pType.getActualTypeArguments();
        if (typeArguments.length != 2 || typeArguments[0] != Long.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException("Field " + field + " has unexpected type params (Key should be of " + Long.class.getName() + " type)");
        }
        if (!codec.canDeserialize(getFieldType(field, true))) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type Navigable<Long,%s> ", field.getName(), field.getDeclaringClass().getName(), field.getDeclaringClass().getName()));
        }
    }

    /**
     * Internal note: This should be in sync with {@link #validateHBColumnMultiVersionField(Field)}
     */
    Type getFieldType(Field field, boolean isMultiVersioned) {
        if (isMultiVersioned) {
            return ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
        } else {
            return field.getGenericType();
        }
    }

    private void validateHBColumnSingleVersionField(Field field) {
        validationHBColumnField(field);
        Type fieldType = getFieldType(field, false);
        if (fieldType instanceof Class) {
            Class fieldClazz = (Class) fieldType;
            if (fieldClazz.isPrimitive()) {
                String suggestion = nativeCounterParts.containsValue(fieldClazz) ? String.format("- Use type %s instead", nativeCounterParts.inverse().get(fieldClazz).getName()) : "";
                throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable) %s", field.getName(), field.getDeclaringClass().getName(), fieldClazz.getName(), suggestion));
            }
        }
        if (!codec.canDeserialize(fieldType)) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s)", field.getName(), field.getDeclaringClass().getName(), fieldType));
        }
    }

    private void validationHBColumnField(Field field) {
        @SuppressWarnings("unchecked")
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(field, hbColumn.getName());
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(field, hbColumn.getName());
        }
    }

    private <R extends Serializable & Comparable<R>> NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> objToMap(HBRecord<R> obj) {
        Class<? extends HBRecord> clazz = obj.getClass();
        validateHBClass(clazz);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
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

    private <R extends Serializable & Comparable<R>> byte[] getFieldValueAsBytes(HBRecord<R> obj, Field field, boolean serializeAsString) {
        Serializable fieldValue;
        try {
            field.setAccessible(true);
            fieldValue = (Serializable) field.get(obj);
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
        return valueToByteArray(fieldValue, serializeAsString);
    }

    private <R extends Serializable & Comparable<R>> NavigableMap<Long, byte[]> getFieldValuesVersioned(Field field, HBRecord<R> obj, boolean serializeAsString) {
        try {
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            NavigableMap<Long, Serializable> fieldValueVersions = (NavigableMap<Long, Serializable>) field.get(obj);
            if (fieldValueVersions == null)
                return null;
            if (fieldValueVersions.size() == 0) {
                throw new FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty();
            }
            NavigableMap<Long, byte[]> output = new TreeMap<>();
            for (NavigableMap.Entry<Long, Serializable> e : fieldValueVersions.entrySet()) {
                Long timestamp = e.getKey();
                Serializable fieldValue = e.getValue();
                if (fieldValue == null)
                    continue;
                byte[] fieldValueBytes = valueToByteArray(fieldValue, serializeAsString);
                output.put(timestamp, fieldValueBytes);
            }
            return output;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Put} object.</p>
     * <p>This method is for use in a MapReduce job whose <code>Reducer</code> class extends HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)</p>
     *
     * @param obj An object of your bean-like class (one that implements {@link HBRecord} interface)
     * @param <R> Data type of row key
     * @return HBase's {@link Put} object
     */
    public <R extends Serializable & Comparable<R>> Put writeValueAsPut(HBRecord<R> obj) {
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
     * A <i>bulk version</i> of {@link #writeValueAsPut(HBRecord)} method
     *
     * @param objects List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @return List of HBase's {@link Put} objects
     */
    public <R extends Serializable & Comparable<R>> List<Put> writeValueAsPut(List<HBRecord<R>> objects) {
        List<Put> puts = new ArrayList<>(objects.size());
        for (HBRecord<R> obj : objects) {
            Put put = writeValueAsPut(obj);
            puts.add(put);
        }
        return puts;
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Result} object.</p>
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input in an HBase table)</p>
     *
     * @param obj object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R> Data type of row key
     * @return HBase's {@link Result} object
     */
    public <R extends Serializable & Comparable<R>> Result writeValueAsResult(HBRecord<R> obj) {
        byte[] row = composeRowKey(obj);
        List<Cell> cellList = new ArrayList<>();
        for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : objToMap(obj).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> columnVersion : valuesVersioned.entrySet()) {
                    cellList.add(new KeyValue(row, family, columnName, columnVersion.getKey(), columnVersion.getValue()));
                }
            }
        }
        return Result.create(cellList);
    }

    /**
     * A <i>bulk version</i> of {@link #writeValueAsResult(HBRecord)} method
     *
     * @param objects List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @return List of HBase's {@link Result} objects
     */
    public <R extends Serializable & Comparable<R>> List<Result> writeValueAsResult(List<HBRecord<R>> objects) {
        List<Result> results = new ArrayList<>(objects.size());
        for (HBRecord<R> obj : objects) {
            Result result = writeValueAsResult(obj);
            results.add(result);
        }
        return results;
    }

    /**
     * <p>Converts HBase's {@link Result} object to an object of your bean-like class.</p>
     * <p>This method is for use in a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input is an HBase table)</p>
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Bean-like object
     * @throws DeserializationException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz) throws DeserializationException {
        if (rowKey == null)
            return readValueFromResult(result, clazz);
        else
            return readValueFromRowAndResult(rowKey.get(), result, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Result, Class)} method
     *
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Bean-like object
     * @throws DeserializationException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Result result, Class<T> clazz) throws DeserializationException {
        return readValueFromResult(result, clazz);
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(R rowKey, Result result, Class<T> clazz) throws DeserializationException {
        if (rowKey == null)
            return readValueFromResult(result, clazz);
        else
            return readValueFromRowAndResult(rowKeyToBytes(rowKey), result, clazz);
    }

    private boolean isResultEmpty(Result result) {
        return result == null || result.isEmpty() || result.getRow() == null || result.getRow().length == 0;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromResult(Result result, Class<T> clazz) throws DeserializationException {
        if (isResultEmpty(result)) return null;
        return mapToObj(result.getRow(), result.getMap(), clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndResult(byte[] rowKey, Result result, Class<T> clazz) throws DeserializationException {
        if (isResultEmpty(result)) return null;
        return mapToObj(rowKey, result.getMap(), clazz);
    }

    private void objectSetFieldValue(Object obj, Field field, NavigableMap<Long, byte[]> columnValuesVersioned, boolean serializeAsString) {
        if (columnValuesVersioned == null)
            return;
        try {
            field.setAccessible(true);
            NavigableMap<Long, Object> columnValuesVersionedBoxed = new TreeMap<>();
            for (NavigableMap.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                columnValuesVersionedBoxed.put(versionAndValue.getKey(), byteArrayToValue(versionAndValue.getValue(), ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1], serializeAsString));
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
    Object byteArrayToValue(byte[] value, Type type, boolean serializeAsString) throws DeserializationException {
        if (value == null || value.length == 0)
            return null;
        Object fieldValue;
        try {
            if (type instanceof Class && fromBytesMethods.containsKey(type)) {
                if (serializeAsString) {
                    Constructor constructor = constructors.get(type);
                    try {
                        fieldValue = constructor.newInstance(Bytes.toString(value));
                    } catch (Exception ex) {
                        fieldValue = null;
                    }
                } else {
                    Method method = fromBytesMethods.get(type);
                    fieldValue = method.invoke(null, new Object[]{value});
                }
            } else {
                return codec.deserialize(value, type);
            }
            return fieldValue;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    /**
     * <p>Converts HBase's {@link Put} object to an object of your bean-like class</p>
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Reducer</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)</p>
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Bean-like object
     * @throws DeserializationException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Put put, Class<T> clazz) throws DeserializationException {
        if (rowKey == null)
            return readValueFromPut(put, clazz);
        else
            return readValueFromRowAndPut(rowKey.get(), put, clazz);
    }


    /**
     * A variant of {@link #readValue(ImmutableBytesWritable, Put, Class)} method
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Bean-like object
     * @throws DeserializationException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(String rowKey, Put put, Class<T> clazz) throws DeserializationException {
        if (rowKey == null)
            return readValueFromPut(put, clazz);
        else
            return readValueFromRowAndPut(Bytes.toBytes(rowKey), put, clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndPut(byte[] rowKey, Put put, Class<T> clazz) throws DeserializationException {
        Map<byte[], List<Cell>> rawMap = put.getFamilyCellMap();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], List<Cell>> familyNameAndColumnValues : rawMap.entrySet()) {
            byte[] family = familyNameAndColumnValues.getKey();
            if (!map.containsKey(family)) {
                map.put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
            }
            List<Cell> cellList = familyNameAndColumnValues.getValue();
            for (Cell cell : cellList) {
                byte[] column = CellUtil.cloneQualifier(cell);
                if (!map.get(family).containsKey(column)) {
                    map.get(family).put(column, new TreeMap<Long, byte[]>());
                }
                map.get(family).get(column).put(cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
        return mapToObj(rowKey, map, clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromPut(Put put, Class<T> clazz) throws DeserializationException {
        if (put == null || put.isEmpty() || put.getRow() == null || put.getRow().length == 0) {
            return null;
        }
        return readValueFromRowAndPut(put.getRow(), put, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Put, Class)} method
     *
     * @param put   HBase's {@link Put} object
     * @param clazz {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Bean-like object
     * @throws DeserializationException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Put put, Class<T> clazz) throws DeserializationException {
        return readValueFromPut(put, clazz);
    }

    /**
     * Get row key (for use in HBase) from a bean-line object.<br>
     * For use in:
     * <ul>
     * <li>reducer jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class</li>
     * <li>unit tests for mapper jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class</li>
     * </ul>
     *
     * @param obj object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R> Data type of row key
     * @return Row key
     */
    public <R extends Serializable & Comparable<R>> ImmutableBytesWritable getRowKey(HBRecord<R> obj) {
        if (obj == null) {
            throw new NullPointerException("Cannot compose row key for null objects");
        }
        return new ImmutableBytesWritable(composeRowKey(obj));
    }

    private <R extends Serializable & Comparable<R>> byte[] composeRowKey(HBRecord<R> obj) {
        R rowKey;
        try {
            rowKey = obj.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.toString().isEmpty()) {
            throw new RowKeyCantBeEmptyException();
        }
        return valueToByteArray(rowKey, false);
    }

    /**
     * Get list of column families mapped in definition of your bean-like class
     *
     * @param clazz {@link Class} that you're reading (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Return set of column families used in input class
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Set<String> getColumnFamilies(Class<T> clazz) {
        validateHBClass(clazz);
        Set<String> columnFamilySet = new HashSet<>();
        for (Field field : clazz.getDeclaredFields()) {
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            if (hbColumn.isPresent())
                columnFamilySet.add(hbColumn.family());
        }
        return columnFamilySet;
    }

    /**
     * Checks whether input class can be converted to HBase data types and vice-versa
     *
     * @param clazz {@link Class} you intend to validate (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return <code>true</code> or <code>false</code>
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean isValid(Class<T> clazz) {
        try {
            validateHBClass(clazz);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * For your bean-like {@link Class}, get all fields mapped to HBase columns
     *
     * @param clazz Bean-like {@link Class} (must implement {@link HBRecord} interface) whose fields you intend to read
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return A {@link Map} with keys as field names and values as instances of {@link Field}
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBFields(Class<T> clazz) {
        validateHBClass(clazz);
        Map<String, Field> mappings = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (new WrappedHBColumn(field).isPresent())
                mappings.put(field.getName(), field);
        }
        return mappings;
    }
}