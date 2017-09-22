package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.Map;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Maps an entity field of type <code>NavigableMap&lt;Long, T&gt;</code> to an HBase column whose data type is represented as data type <code>T</code>.
 * <p>
 * As the name explains, this annotation is the multi-version variant of {@link HBColumn}.
 * <p>
 * <b>Please note</b>: <code>T</code> must be {@link Serializable}
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface HBColumnMultiVersion {

    /**
     * Name of HBase column family
     *
     * @return Name of HBase column family
     */
    String family();

    /**
     * Name of HBase column
     *
     * @return Name of HBase column
     */
    String column();

    /**
     * <b>[optional]</b> flags to be passed to codec's {@link Codec#serialize(Serializable, Map) serialize} and {@link Codec#deserialize(byte[], Type, Map) deserialize} methods
     * <p>
     * Note: These flags will be passed as a <code>Map&lt;String, String&gt;</code> (param name and param value)
     *
     * @return Flags
     */
    Flag[] codecFlags() default {};

}