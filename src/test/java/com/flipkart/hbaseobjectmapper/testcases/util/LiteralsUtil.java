package com.flipkart.hbaseobjectmapper.testcases.util;

import com.google.common.collect.Sets;

import java.util.*;

public class LiteralsUtil {

    @SafeVarargs
    public static <T> T[] a(T... a) {
        return a;
    }

    @SafeVarargs
    public static <T> List<T> l(T... a) {
        return Arrays.asList(a);
    }

    @SafeVarargs
    public static <T> Set<T> s(T... a) {
        return Sets.newHashSet(a);
    }

    public static <K, V> Map.Entry<K, V> e(K key, V value) {
        return new HashMap.SimpleEntry<>(key, value);
    }

    @SafeVarargs
    public static <K, V> Map<K, V> m(final Map.Entry<K, V>... entries) {
        return new HashMap<K, V>(entries.length, 1.0f) {
            {
                for (final Map.Entry<K, V> entry : entries)
                    put(entry.getKey(), entry.getValue());
            }
        };
    }

    @SafeVarargs
    public static <K, V> NavigableMap<K, V> nm(final Map.Entry<K, V>... entries) {
        return new TreeMap<K, V>() {
            {
                for (final Map.Entry<K, V> entry : entries)
                    put(entry.getKey(), entry.getValue());
            }
        };
    }

}
