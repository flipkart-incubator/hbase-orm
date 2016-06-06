package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mrunit.types.Pair;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestUtil {
    private static final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    public static Pair<ImmutableBytesWritable, Result> writeValueAsRowKeyResultPair(HBRecord obj) {
        return new Pair<ImmutableBytesWritable, Result>(hbObjectMapper.getRowKey(obj), hbObjectMapper.writeValueAsResult(obj));
    }

    public static List<Pair<ImmutableBytesWritable, Result>> writeValueAsRowKeyResultPair(List<? extends HBRecord> objs) {
        List<Pair<ImmutableBytesWritable, Result>> pairList = new ArrayList<Pair<ImmutableBytesWritable, Result>>(objs.size());
        for (HBRecord obj : objs) {
            pairList.add(writeValueAsRowKeyResultPair(obj));
        }
        return pairList;
    }

    public static <T> boolean setEquals(Set<T> leftSet, Set<T> rightSet) {
        return !(leftSet == null || rightSet == null || leftSet.size() != rightSet.size()) && rightSet.containsAll(leftSet);
    }

    public static <K, V> boolean mapEquals(Map<K, V> leftMap, Map<K, V> rightMap) {
        if (leftMap == null || rightMap == null || leftMap.size() != rightMap.size()) return false;
        for (K key : leftMap.keySet()) {
            V value1 = leftMap.get(key);
            V value2 = rightMap.get(key);
            if (!value1.equals(value2)) return false;
        }
        return true;
    }

    public static Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> triplet(HBRecord record, String classDescription, Class<? extends IllegalArgumentException> clazz) {
        return new Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>(record, classDescription, clazz);
    }


}
