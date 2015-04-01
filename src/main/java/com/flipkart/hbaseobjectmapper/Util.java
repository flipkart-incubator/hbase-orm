package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class Util {
    public static ImmutableBytesWritable strToIbw(String str) {
        return str == null ? null : new ImmutableBytesWritable(Bytes.toBytes(str));
    }

    public static List<ImmutableBytesWritable> strToIbw(Iterable<String> strList) {
        List<ImmutableBytesWritable> ibwList = new ArrayList<ImmutableBytesWritable>();
        for (String str : strList) {
            ibwList.add(strToIbw(str));
        }
        return ibwList;
    }
}
