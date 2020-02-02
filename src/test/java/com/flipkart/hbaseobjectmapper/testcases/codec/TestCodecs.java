package com.flipkart.hbaseobjectmapper.testcases.codec;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.codec.*;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.CodecException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeComposedException;
import com.flipkart.hbaseobjectmapper.testcases.TestObjects;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import org.apache.hadoop.hbase.client.Put;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("unchecked")
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
            for (HBRecord record : TestObjects.validObjects) {
                Class<? extends HBRecord> objectClass = record.getClass();
                final Serializable rowKey = record.composeRowKey();
                final Map<String, String> rowKeyCodecFlags = toMap(objectClass.getAnnotation(HBTable.class).rowKeyCodecFlags());
                byte[] bytes = codec.serialize(rowKey, rowKeyCodecFlags);
                Serializable deserializedRowKey = codec.deserialize(bytes, rowKey.getClass(), rowKeyCodecFlags);
                assertEquals(rowKey, deserializedRowKey, String.format("Row key got corrupted after serialization and deserialization, for this record:%n%s%n", record));
                for (Object re : hbObjectMapper.getHBColumnFields(objectClass).entrySet()) {
                    Map.Entry<String, Field> e = (Map.Entry<String, Field>) re;
                    String fieldName = e.getKey();
                    Field field = e.getValue();
                    field.setAccessible(true);
                    if (field.isAnnotationPresent(HBColumnMultiVersion.class)) {
                        final Type actualFieldType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
                        Object fieldValuesMap = field.get(record);
                        if (fieldValuesMap == null)
                            continue;
                        for (NavigableMap.Entry<Long, ?> entry : ((NavigableMap<Long, ?>) fieldValuesMap).entrySet()) {
                            verifyFieldSerDe(codec, objectClass.getSimpleName() + "." + fieldName, actualFieldType, (Serializable) entry.getValue(), toMap(field.getAnnotation(HBColumnMultiVersion.class).codecFlags()));
                        }
                    } else {
                        Serializable fieldValue = (Serializable) field.get(record);
                        verifyFieldSerDe(codec, objectClass.getSimpleName() + "." + fieldName, field.getGenericType(), fieldValue, toMap(field.getAnnotation(HBColumn.class).codecFlags()));
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
        if (codecFlags == null)
            return null;
        Map<String, String> flagsMap = new HashMap<>();
        for (Flag flag : codecFlags) {
            flagsMap.put(flag.name(), flag.value());
        }
        return flagsMap;
    }


    private void verifyFieldSerDe(Codec codec, String fieldFullName, Type type, Serializable fieldValue, Map<String, String> flags) throws SerializationException, DeserializationException {
        byte[] bytes = codec.serialize(fieldValue, flags);
        Serializable deserializedFieldValue = codec.deserialize(bytes, type, flags);
        assertEquals(fieldValue, deserializedFieldValue,
                String.format("Field %s got corrupted after serialization and deserialization of it's value:%n%s%n", fieldFullName, fieldValue)
        );
    }

    @Test
    public void testSerializationFailure() {
        HBObjectMapper hbObjectMapper = new HBObjectMapper(new Codec() {
            @Override
            public byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException {
                if (object.equals("key")) {
                    throw new RowKeyCantBeComposedException(null);
                } else {
                    throw new SerializationException("Dummy exception", null);

                }
            }

            @Override
            public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
                return null;
            }

            @Override
            public boolean canDeserialize(Type type) {
                return true;
            }
        });
        try {
            hbObjectMapper.writeValueAsPut(TestObjects.validObjects.get(0));
        } catch (CodecException e) {
            assertEquals(SerializationException.class, e.getCause().getClass(),
                    String.format("Cause of %s should'be been %s", CodecException.class.getSimpleName(), SerializationException.class.getSimpleName())
            );
        }
    }

    @Test
    public void testDeserializationFailure() {
        HBObjectMapper hbObjectMapper = new HBObjectMapper(new BestSuitCodec() {

            @Override
            public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
                throw new DeserializationException("Dummy exception", null);
            }
        });
        Put put = hbObjectMapper.writeValueAsPut(TestObjects.validObjects.get(0));
        try {
            System.out.println(hbObjectMapper.readValue(put, Citizen.class));
            fail("Trying to serialize corrupt data should've thrown " + CodecException.class.getSimpleName());
        } catch (CodecException e) {
            assertEquals(DeserializationException.class, e.getCause().getClass(),
                    String.format("Cause of %s should'be been %s", CodecException.class.getSimpleName(), DeserializationException.class.getSimpleName())
            );
        }
    }
}
