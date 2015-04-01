package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBColumn {

    /**
     * Name of HBase column family
     */
    String family();

    /**
     * Name of HBase column
     */
    String column();

    /**
     * (Applicable to numeric fields) Store field value in it's string representation (e.g. (int)560034 is stored as "560034")
     */
    boolean serializeAsString() default false;
}
