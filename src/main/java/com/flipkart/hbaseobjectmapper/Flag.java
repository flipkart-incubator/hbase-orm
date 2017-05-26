package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A flag for {@link Codec Codec} (specify parameter name and value)
 * <p>
 * This is to be used exclusively for input to {@link HBColumn#codecFlags() codecFlags} parameter of {@link HBColumn} and {@link HBColumnMultiVersion} annotations
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Flag {
    String name();

    String value();
}
