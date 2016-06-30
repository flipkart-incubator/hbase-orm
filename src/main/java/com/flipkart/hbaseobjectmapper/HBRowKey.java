package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated field (in part or full) forms row key. This is to be treated as a 'marker' annotation. Actual row key composition solely depends on your implementation of {@link HBRecord#composeRowKey()}
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBRowKey {
}
