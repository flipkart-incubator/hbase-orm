package com.flipkart.hbaseobjectmapper;

import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * This test case makes sure that annotations {@link HBColumn} and {@link HBColumnMultiVersion} are structurally same.
 * <p>
 * As Java doesn't allow inheritance of (<code>extends</code> on) annotations and we're manually making sure above annotations have same members etc, this test case is important.
 */
public class TestHBColumnAnnotationsSymmetry {
    @Test
    public void testParameterSymmetry() throws NoSuchMethodException {
        String message = String.format("Annotations %s and %s differ in their parameters", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName());
        assertEquals(message, HBColumn.class.getDeclaredMethods().length, HBColumnMultiVersion.class.getDeclaredMethods().length);
        for (Method svMethod : HBColumn.class.getDeclaredMethods()) {
            Method mvMethod = HBColumnMultiVersion.class.getMethod(svMethod.getName());
            assertEquals(message, svMethod.getReturnType(), mvMethod.getReturnType());
        }
    }

    @Test
    public void testAnnotationSymmetry() {
        Annotation[] svAnnotations = HBColumn.class.getAnnotations(), mvAnnotations = HBColumnMultiVersion.class.getAnnotations();
        Comparator<Annotation> annotationComparator = new Comparator<Annotation>() {
            @Override
            public int compare(Annotation a1, Annotation a2) {
                return a1.annotationType().getName().compareTo(a2.annotationType().getName());
            }
        };
        Arrays.sort(svAnnotations, annotationComparator);
        Arrays.sort(mvAnnotations, annotationComparator);
        assertArrayEquals(String.format("Annotations %s and %s differ in annotations on them", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()), svAnnotations, mvAnnotations);
    }
}
