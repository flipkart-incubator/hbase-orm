package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.Map;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Maps an entity class to a table in HBase
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface HBTable {
    /**
     * Name of the HBase table
     *
     * @return Name of the HBase table
     */
    String name();


    /**
     * Column families and their specs
     *
     * @return Column families and their specs
     */
    Family[] families() default {};

    /**
     * <b>[optional]</b> flags to be passed to codec's {@link Codec#serialize(Serializable, Map) serialize} and {@link Codec#deserialize(byte[], Type, Map) deserialize} methods
     * <p>
     * Note: These flags will be passed as a <code>Map&lt;String, String&gt;</code> (param name and param value)
     *
     * @return Flags
     */
    Flag[] rowKeyCodecFlags() default {};
}
