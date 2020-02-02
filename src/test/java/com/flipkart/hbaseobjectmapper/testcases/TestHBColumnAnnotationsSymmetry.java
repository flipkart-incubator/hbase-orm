package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test case makes sure that annotations {@link HBColumn} and {@link HBColumnMultiVersion} are structurally same.
 * <p>
 * As Java doesn't allow inheritance of (<code>extends</code> on) annotations and we're manually making sure above annotations have same members etc, this test case is important.
 */
public class TestHBColumnAnnotationsSymmetry {
    @Test
    public void testParameterSymmetry() throws NoSuchMethodException {
        String message = String.format("Annotations %s and %s differ in their methods", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName());
        assertEquals(HBColumn.class.getDeclaredMethods().length, HBColumnMultiVersion.class.getDeclaredMethods().length, message);
        for (Method svMethod : HBColumn.class.getDeclaredMethods()) {
            Method mvMethod = HBColumnMultiVersion.class.getMethod(svMethod.getName());
            assertEquals(svMethod.getReturnType(), mvMethod.getReturnType(), String.format("%s (check return type of method %s)", message, svMethod.getName()));
            assertArrayEquals(svMethod.getParameterTypes(), mvMethod.getParameterTypes(), String.format("%s (check parameter types of method %s)", message, svMethod.getName()));
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
        assertArrayEquals(svAnnotations, mvAnnotations, String.format("Annotations %s and %s differ in annotations on them", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()));
    }
}
