package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates that the annotated field (in part or full) forms row key.
 * <p>
 * This is just as a 'marker' annotation. Actual row key composition solely depends on your implementation of {@link HBRecord#composeRowKey()} method
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface HBRowKey {
}
