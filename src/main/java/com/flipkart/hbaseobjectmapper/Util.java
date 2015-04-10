package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class
 */
public class Util {
    /**
     * Converts a {@link String} to {@link ImmutableBytesWritable} object
     */
    public static ImmutableBytesWritable strToIbw(String str) {
        return str == null ? null : new ImmutableBytesWritable(Bytes.toBytes(str));
    }

    /**
     * Converts a list of {@link String}s to a list of {@link ImmutableBytesWritable} objects
     */
    public static List<ImmutableBytesWritable> strToIbw(Iterable<String> strList) {
        List<ImmutableBytesWritable> ibwList = new ArrayList<ImmutableBytesWritable>();
        for (String str : strList) {
            ibwList.add(strToIbw(str));
        }
        return ibwList;
    }

    /**
     * Converts an {@link ImmutableBytesWritable} object to {@link String}
     */
    public static String ibwToStr(ImmutableBytesWritable ibw) {
        return ibw == null ? null : Bytes.toString(ibw.get());
    }
}
