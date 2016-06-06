package com.flipkart.hbaseobjectmapper.codec;

import java.io.*;
import java.lang.reflect.Type;

public class JavaObjectStreamCodec implements Codec {
    public byte[] serialize(Serializable object) throws SerializationException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Could not serialise object to byte stream", e);
        }
    }

    @Override
    public Serializable deserialize(byte[] bytes, Type type) throws DeserializationException {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (Serializable) ois.readObject();
        } catch (Exception e) {
            throw new DeserializationException("Could not deserialise byte stream into an object", e);
        }
    }

    @Override
    public boolean canDeserialize(Type type) {
        return true;
    }

}
