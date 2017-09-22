package com.flipkart.hbaseobjectmapper.codec;


import com.flipkart.hbaseobjectmapper.HBObjectMapper;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Interface that defines serialization and deserialization behavior for {@link HBObjectMapper HBObjectMapper}
 */
public interface Codec {
    /**
     * Serializes object to a <code>byte[]</code>
     *
     * @param object Object to be serialized
     * @param flags  Flags for tuning serialization behavior (Implementations of this method are expected to handle <code>null</code> and <code>empty map</code> in the same way)
     * @return byte array - this would be used 'as is' in setting the column value in HBase row
     * @throws SerializationException If serialization fails (e.g. when input <code>object</code> has a field of data type that isn't serializable by this codec)
     * @see #deserialize(byte[], Type, Map)
     */
    byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException;

    /**
     * Deserialize <code>byte[]</code> into an object
     *
     * @param bytes byte array that needs to be deserialized
     * @param flags Flags for tuning deserialization behavior  (Implementations of this method are expected to handle <code>null</code> and <code>empty map</code> in the same way)
     * @param type  Java type to which this <code>byte[]</code> needs to be deserialized to
     * @return The object
     * @throws DeserializationException If deserialization fails (e.g. malformed string or definition of a data type used isn't available at runtime)
     * @see #serialize(Serializable, Map)
     * @see #canDeserialize(Type)
     */
    Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException;

    /**
     * Check whether a specific type can be deserialized using this codec
     *
     * @param type Java type
     * @return <code>true</code> (if an object of specified type can be deserialized using this codec) or <code>false</code>
     * @see #deserialize(byte[], Type, Map)
     */
    boolean canDeserialize(Type type);

}
