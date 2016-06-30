package com.flipkart.hbaseobjectmapper.codec;


import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.TestObjects;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
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
    public void testJacksonJsonCodec() {
        testWithCodec(new JacksonJsonCodec());
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
                            verifySerDe(codec, objectClass.getSimpleName() + "." + fieldName, entry.getValue().getClass(), (Serializable) entry.getValue());
                        }
                    } else {
                        Serializable fieldValue = (Serializable) field.get(object);
                        verifySerDe(codec, objectClass.getSimpleName() + "." + fieldName, field.getType(), fieldValue);
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

    private void verifySerDe(Codec codec, String fieldFullName, Type type, Serializable fieldValue) throws SerializationException, DeserializationException {
        byte[] bytes = codec.serialize(fieldValue);
        Serializable deserializedFieldValue = codec.deserialize(bytes, type);
        assertEquals(String.format("Field %s got corrupted after serialization and deserialization of it's value:\n%s\n", fieldFullName, fieldValue), fieldValue, deserializedFieldValue);
    }
}
