package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.ArrayList;
import java.util.List;

public class TestUtil {
    private static HBObjectMapper hbObjectMapper = new HBObjectMapper();

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
}
