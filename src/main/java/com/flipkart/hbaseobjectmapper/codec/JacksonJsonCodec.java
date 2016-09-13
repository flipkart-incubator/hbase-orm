package com.flipkart.hbaseobjectmapper.codec;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.lang.reflect.Type;

public class JacksonJsonCodec implements Codec {

    private final ObjectMapper objectMapper;

    public JacksonJsonCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JacksonJsonCodec() {
        this(new ObjectMapper());
    }

    /*
    * @inherit
    */
    @Override
    public byte[] serialize(Serializable object) throws SerializationException {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new SerializationException("Could not serialise object to JSON (using Jackson)", e);
        }
    }

    /*
    * @inherit
    */
    @Override
    public Serializable deserialize(byte[] bytes, Type type) throws DeserializationException {
        try {
            return objectMapper.readValue(bytes, objectMapper.constructType(type));
        } catch (Exception e) {
            throw new DeserializationException("Could not deserialise JSON into an object (using Jackson)", e);
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
}
