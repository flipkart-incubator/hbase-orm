package com.flipkart.hbaseobjectmapper.codec;

import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Just a reference implementation, kept here for testing purposes. In real world, you should <b>never</b> use this codec. Either use the (default) {@link BestSuitCodec} or write your own.
 */
public class JavaObjectStreamCodec implements Codec {
    /*
     * @inherit
     */
    @Override
    public byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Could not serialize object to byte stream", e);
        }
    }

    /*
     * @inherit
     */
    @Override
    public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (Serializable) ois.readObject();
        } catch (Exception e) {
            throw new DeserializationException("Could not deserialize byte array to object", e);
        }
    }

    /*
     * @inherit
     */
    @Override
    public boolean canDeserialize(Type type) {
        return true; // I'm (may be wrongly) assuming ObjectInputStream and ObjectOutputStream work on all objects
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deepCopy(T object) {
        final JavaObjectStreamCodec codec = new JavaObjectStreamCodec();
        try {
            final byte[] bytes = codec.serialize(object, null);
            return (T) codec.deserialize(bytes, object.getClass(), null);
        } catch (Exception e) {
            throw new IllegalStateException("Could not deep copy: " + object, e);
        }
    }

}
