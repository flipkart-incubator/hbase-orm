package com.flipkart.hbaseobjectmapper.codec;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flipkart.hbaseobjectmapper.Flag;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
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
 * <li><b><code>{@link #SERIALIZE_AS_STRING}</code></b>: When this flag is "true", this codec stores field/rowkey values in it's string representation (e.g. <b>560034</b> is serialized into a <code>byte[]</code> that represents the string <b>"560034"</b>). This flag applies only to fields or rowkeys of data types in point 1 above.</li>
 * </ul>
 * <p>
 * This is the default codec for {@link com.flipkart.hbaseobjectmapper.HBObjectMapper HBObjectMapper}.
 */

public class BestSuitCodec implements Codec {
    public static final String SERIALIZE_AS_STRING = "serializeAsString";

    private final ObjectMapper objectMapper;

    /**
     * Construct an object of class {@link BestSuitCodec} with custom instance of Jackson's Object Mapper
     *
     * @param objectMapper Instance of Jackson's Object Mapper
     */
    @SuppressWarnings("WeakerAccess")
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
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(new Jdk8Module());
        return objectMapper;
    }

    /*
     * @inherit
     */
    @Override
    public byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException {
        if (object == null) {
            return null;
        }
        Class<?> clazz = object.getClass();
        if (isSerializeAsStringTrue(flags)) {
            object = String.valueOf(object);
            clazz = String.class;
        }
        try {
            if (clazz == String.class) {
                return Bytes.toBytes((String) object);
            } else if (clazz == Integer.class) {
                return Bytes.toBytes((int) object);
            } else if (clazz == Short.class) {
                return Bytes.toBytes((short) object);
            } else if (clazz == Long.class) {
                return Bytes.toBytes((long) object);
            } else if (clazz == Float.class) {
                return Bytes.toBytes((float) object);
            } else if (clazz == Double.class) {
                return Bytes.toBytes((double) object);
            } else if (clazz == BigDecimal.class) {
                return Bytes.toBytes((BigDecimal) object);
            } else if (clazz == Boolean.class) {
                return Bytes.toBytes((boolean) object);
            }
        } catch (Exception e) {
            throw new SerializationException(String.format("Could not serialize value of type %s using HBase's native methods", clazz.getName()), e);
        }
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new SerializationException("Could not serialize object to JSON using Jackson", e);
        }
    }

    /*
     * @inherit
     */
    @Override
    public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
        if (bytes == null)
            return null;
        boolean serializeAsString = isSerializeAsStringTrue(flags);
        if (type instanceof Class) {
            if (serializeAsString) {
                try {
                    String string = Bytes.toString(bytes);
                    if (type == Integer.class) {
                        return Integer.valueOf(string);
                    } else if (type == Long.class) {
                        return Long.valueOf(string);
                    } else if (type == Short.class) {
                        return Short.valueOf(string);
                    } else if (type == Float.class) {
                        return Float.valueOf(string);
                    } else if (type == Double.class) {
                        return Double.valueOf(string);
                    } else if (type == BigDecimal.class) {
                        return new BigDecimal(string);
                    } else if (type == Boolean.class) {
                        return Boolean.valueOf(string);
                    }
                } catch (Exception e) {
                    throw new DeserializationException("Could not deserialize byte array into an object using HBase's native methods (note: serialize as string is on)", e);
                }
            } else {
                try {
                    if (type == String.class) {
                        return Bytes.toString(bytes);
                    } else if (type == Integer.class) {
                        return Bytes.toInt(bytes);
                    } else if (type == Long.class) {
                        return Bytes.toLong(bytes);
                    } else if (type == Short.class) {
                        return Bytes.toShort(bytes);
                    } else if (type == Float.class) {
                        return Bytes.toFloat(bytes);
                    } else if (type == Double.class) {
                        return Bytes.toDouble(bytes);
                    } else if (type == BigDecimal.class) {
                        return Bytes.toBigDecimal(bytes);
                    } else if (type == Boolean.class) {
                        return Bytes.toBoolean(bytes);
                    }
                } catch (Exception e) {
                    throw new DeserializationException("Could not deserialize byte array into an object using HBase's native methods", e);
                }
            }
        }
        JavaType javaType = null;
        try {
            javaType = objectMapper.constructType(type);
            return objectMapper.readValue(bytes, javaType);
        } catch (Exception e) {
            throw new DeserializationException(String.format("Could not deserialize JSON into an object of type %s using Jackson%n(Jackson resolved type = %s)", type, javaType), e);
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

    private static boolean isSerializeAsStringTrue(Map<String, String> flags) {
        return flags != null && flags.get(SERIALIZE_AS_STRING) != null && flags.get(SERIALIZE_AS_STRING).equalsIgnoreCase("true");
    }
}
