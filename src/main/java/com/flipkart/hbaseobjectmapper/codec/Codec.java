package com.flipkart.hbaseobjectmapper.codec;


import java.io.Serializable;
import java.lang.reflect.Type;

/**
 * Interface to control serialization and deserialization within {@link com.flipkart.hbaseobjectmapper.HBObjectMapper HBObjectMapper}
 */
public interface Codec {
    /**
     * Serialize object to a <code>byte[]</code>
     *
     * @param object Object to be serialized
     * @return byte array - this would be used as is in setting column value
     * @throws SerializationException If serialization fails (e.g. classes uses a type that isn't serializable by this codec)
     */
    byte[] serialize(Serializable object) throws SerializationException;

    /**
     * Deserialize <code>byte[]</code> into object
     *
     * @param bytes byte array that represents serialized object
     * @param type  Java type to which this <code>byte[]</code> needs to be deserialized to
     * @return object
     * @throws DeserializationException If deserialization fails (e.g. malformed string or definition of a type used isn't available at runtime)
     */
    Serializable deserialize(byte[] bytes, Type type) throws DeserializationException;

    /**
     * Check whether a specific type can be deserialized using this codec
     *
     * @param type Java type
     * @return <code>true</code> or <code>false</code>
     */
    boolean canDeserialize(Type type);

}
