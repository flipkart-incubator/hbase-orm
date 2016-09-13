package com.flipkart.hbaseobjectmapper.util;

import com.flipkart.hbaseobjectmapper.HBRecord;
import com.google.common.collect.Sets;
import org.javatuples.Triplet;

import java.util.Set;

public class TestUtil {

    public static Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> triplet(HBRecord record, String classDescription, Class<? extends IllegalArgumentException> clazz) {
        return new Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>(record, classDescription, clazz);
    }

    @SafeVarargs
    public static <T> T[] a(T... a) {
        return a;
    }

    @SafeVarargs
    public static <T> Set<T> s(T... a) {
        return Sets.newHashSet(a);
    }
}
