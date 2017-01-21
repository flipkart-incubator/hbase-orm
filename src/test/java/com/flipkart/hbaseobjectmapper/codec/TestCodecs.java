package com.flipkart.hbaseobjectmapper.codec;


import com.flipkart.hbaseobjectmapper.*;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCodecs {

    @Test
    public void testJavaObjectStreamCodec() {
        testWithCodec(new JavaObjectStreamCodec());
    }

    @Test
    public void testBestSuitCodec() {
        testWithCodec(new BestSuitCodec());
    }

    @SuppressWarnings("unchecked")
    public void testWithCodec(Codec codec) {
        HBObjectMapper hbObjectMapper = new HBObjectMapper(codec);
        try {
            for (HBRecord object : TestObjects.validObjects) {
                Class<? extends HBRecord> objectClass = object.getClass();
                for (Object re : hbObjectMapper.getHBFields(objectClass).entrySet()) {
                    Map.Entry<String, Field> e = (Map.Entry<String, Field>) re;
                    String fieldName = e.getKey();
                    Field field = e.getValue();
                    field.setAccessible(true);
                    if (field.isAnnotationPresent(HBColumnMultiVersion.class)) {
                        Object fieldValuesMap = field.get(object);
                        if (fieldValuesMap == null)
                            continue;
                        for (NavigableMap.Entry<Long, ?> entry : ((NavigableMap<Long, ?>) fieldValuesMap).entrySet()) {
                            verifySerDe(codec, objectClass.getSimpleName() + "." + fieldName, entry.getValue().getClass(), (Serializable) entry.getValue(), toMap(field.getAnnotation(HBColumnMultiVersion.class).codecFlags()));
                        }
                    } else {
                        Serializable fieldValue = (Serializable) field.get(object);
                        verifySerDe(codec, objectClass.getSimpleName() + "." + fieldName, field.getType(), fieldValue, toMap(field.getAnnotation(HBColumn.class).codecFlags()));
                    }
                }
            }
        } catch (SerializationException e) {
            e.printStackTrace();
            fail("Error during serialization");
        } catch (DeserializationException e) {
            e.printStackTrace();
            fail("Error during deserialization");
        } catch (IllegalAccessException iae) {
            iae.printStackTrace();
            fail();
        }

    }

    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>();
        for (Flag flag : codecFlags) {
            flagsMap.put(flag.name(), flag.value());
        }
        return flagsMap;
    }


    private void verifySerDe(Codec codec, String fieldFullName, Type type, Serializable fieldValue, Map<String, String> flags) throws SerializationException, DeserializationException {
        byte[] bytes = codec.serialize(fieldValue, flags);
        Serializable deserializedFieldValue = codec.deserialize(bytes, type, flags);
        assertEquals(String.format("Field %s got corrupted after serialization and deserialization of it's value:\n%s\n", fieldFullName, fieldValue), fieldValue, deserializedFieldValue);
    }
}
