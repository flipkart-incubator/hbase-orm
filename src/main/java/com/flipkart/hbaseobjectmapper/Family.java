package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents a column family in HBase
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
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
     * @return Max mumber of versions
     */
    int versions() default 1;
}
