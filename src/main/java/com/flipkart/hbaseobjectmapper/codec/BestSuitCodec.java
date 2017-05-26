package com.flipkart.hbaseobjectmapper.codec;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.hbaseobjectmapper.Flag;
import com.flipkart.hbaseobjectmapper.exceptions.BadHBaseLibStateException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * This is an implementation of {@link Codec} that:
 * <ol>
 * <li>uses HBase's native methods to serialize objects of data types {@link Boolean}, {@link Short}, {@link Integer}, {@link Long}, {@link Float}, {@link Double}, {@link String} and {@link BigDecimal}</li>
 * <li>uses Jackson's JSON serializer for all other data types</li>
 * <li>serializes <code>null</code> as <code>null</code></li>
 * </ol>
 * <p>
 * This codec takes the following {@link Flag Flag}s:
 * <ul>
 * <li><b><code>{@link #SERIALISE_AS_STRING}</code></b>: When passed, it indicates this codec to store field value in it's string representation (e.g. <b>560034</b> is serialized into a <code>byte[]</code> that represents the string <b>"560034"</b>). Note that, this flag applies only to fields of data types in point 1 above.</li>
 * </ul>
 */

public class BestSuitCodec implements Codec {
    public static final String SERIALISE_AS_STRING = "serializeAsString";

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


    private final ObjectMapper objectMapper;

    /**
     * Construct an object of class {@link BestSuitCodec} with custom instance of Jackson's Object Mapper
     *
     * @param objectMapper Instance of Jackson's Object Mapper
     */
    public BestSuitCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Construct an object of class {@link BestSuitCodec}
     */
    public BestSuitCodec() {
        this(getObjectMapper());
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }

    /*
    * @inherit
    */
    @Override
    public byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException {
        if (object == null)
            return null;
        Class clazz = object.getClass();
        if (toBytesMethods.containsKey(clazz)) {
            boolean serializeAsString = isSerializeAsStringOn(flags);
            try {
                Method toBytesMethod = toBytesMethods.get(clazz);
                return serializeAsString ? Bytes.toBytes(String.valueOf(object)) : (byte[]) toBytesMethod.invoke(null, object);
            } catch (Exception e) {
                throw new SerializationException(String.format("Could not serialize value of type %s using HBase's native methods", clazz.getName()), e);
            }
        } else {
            try {
                return objectMapper.writeValueAsBytes(object);
            } catch (Exception e) {
                throw new SerializationException("Could not serialize object to JSON using Jackson", e);
            }
        }
    }

    /*
    * @inherit
    */
    @Override
    public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
        if (bytes == null)
            return null;
        if (type instanceof Class && fromBytesMethods.containsKey(type)) {
            boolean serializeAsString = isSerializeAsStringOn(flags);
            try {
                Serializable fieldValue;
                if (serializeAsString) {
                    Constructor constructor = constructors.get(type);
                    fieldValue = (Serializable) constructor.newInstance(Bytes.toString(bytes));
                } else {
                    Method method = fromBytesMethods.get(type);
                    fieldValue = (Serializable) method.invoke(null, new Object[]{bytes});
                }
                return fieldValue;
            } catch (Exception e) {
                throw new DeserializationException("Could not deserialize byte array into an object using HBase's native methods", e);
            }
        } else {
            JavaType javaType = null;
            try {
                javaType = objectMapper.constructType(type);
                return objectMapper.readValue(bytes, javaType);
            } catch (Exception e) {
                throw new DeserializationException(String.format("Could not deserialize JSON into an object of type %s using Jackson\n(Jackson resolved type = %s)", type, javaType), e);
            }
        }

    }

    /*
    * @inherit
    */
    @Override
    public boolean canDeserialize(Type type) {
        JavaType javaType = objectMapper.constructType(type);
        return objectMapper.canDeserialize(javaType);
    }

    private boolean isSerializeAsStringOn(Map<String, String> flags) {
        return flags != null && flags.get(SERIALISE_AS_STRING) != null && flags.get(SERIALISE_AS_STRING).equalsIgnoreCase("true");
    }
}
