package com.flipkart.hbaseobjectmapper.mr.lib;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class TableReduceDriver<KEYIN, VALUEIN, KEYOUT> extends ReduceDriver<KEYIN, VALUEIN, KEYOUT, Writable> {
    public TableReduceDriver() {
        super();
    }

    public TableReduceDriver(final Reducer<KEYIN, VALUEIN, KEYOUT, Writable> r) {
        super(r);
    }

    public static <KEYIN, VALUEIN, KEYOUT> TableReduceDriver<KEYIN, VALUEIN, KEYOUT> newTableReduceDriver() {
        return new TableReduceDriver<KEYIN, VALUEIN, KEYOUT>();
    }

    public static <KEYIN, VALUEIN, KEYOUT> TableReduceDriver<KEYIN, VALUEIN, KEYOUT> newTableReduceDriver(final Reducer<KEYIN, VALUEIN, KEYOUT, Writable> r) {
        return new TableReduceDriver<KEYIN, VALUEIN, KEYOUT>(r);
    }


}
