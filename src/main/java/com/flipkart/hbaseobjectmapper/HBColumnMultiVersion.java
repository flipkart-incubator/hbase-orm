package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Maps an entity field of type <code>NavigableMap&lt;Long, T&gt;</code> to an HBase column whose data type is represented as data type <code>T</code>.
 * <p>
 * As the name explains, this annotation is the multi-version variant of {@link HBColumn}.
 * <p>
 * <b>Please note</b>: <code>T</code> must be {@link Serializable}
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBColumnMultiVersion {

    /**
     * Name of HBase column family
     */
    String family();

    /**
     * Name of HBase column
     */
    String column();

    /**
     * <b>[optional]</b> flags to be passed to codec's {@link com.flipkart.hbaseobjectmapper.codec.Codec#serialize(Serializable, Map) serialize} and {@link com.flipkart.hbaseobjectmapper.codec.Codec#deserialize(byte[], Type, Map) deserialize} methods
     * <p>
     * Note: These flags will be passed as a <code>Map&lt;String, String&gt;</code> (param name and param value)
     */
    Flag[] codecFlags() default {};

}
