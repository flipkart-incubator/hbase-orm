package com.flipkart.hbaseobjectmapper.codec;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;

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
            throw new DeserializationException("Could not deserialize byte stream into an object", e);
        }
    }

    /*
    * @inherit
    */
    @Override
    public boolean canDeserialize(Type type) {
        return true;
    }

    public static Serializable deepCopy(Serializable object) {
        final JavaObjectStreamCodec codec = new JavaObjectStreamCodec();
        try {
            final byte[] bytes = codec.serialize(object, null);
            return codec.deserialize(bytes, object.getClass(), null);
        } catch (Exception e) {
            throw new IllegalStateException("Could not deep copy: " + object, e);
        }
    }

}
