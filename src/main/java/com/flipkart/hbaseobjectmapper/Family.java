package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Represents a column family in HBase
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Family {
    /**
     * Column family name
     *
     * @return Column family name
     */
    String name();

    /**
     * Maximum number of versions configured for a given column family of the HBase table
     *
     * @return Max number of versions
     */
    int versions() default 1;
}
