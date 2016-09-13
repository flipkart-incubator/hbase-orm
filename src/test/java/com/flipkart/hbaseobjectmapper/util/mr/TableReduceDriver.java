package com.flipkart.hbaseobjectmapper.util.mr;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class TableReduceDriver<KEYIN, VALUEIN, KEYOUT> extends ReduceDriver<KEYIN, VALUEIN, KEYOUT, Mutation> {
    public TableReduceDriver() {
        super();
    }

    public TableReduceDriver(final Reducer<KEYIN, VALUEIN, KEYOUT, Mutation> r) {
        super(r);
    }

    public static <KEYIN, VALUEIN, KEYOUT> TableReduceDriver<KEYIN, VALUEIN, KEYOUT> newTableReduceDriver() {
        return new TableReduceDriver<>();
    }

    public static <KEYIN, VALUEIN, KEYOUT> TableReduceDriver<KEYIN, VALUEIN, KEYOUT> newTableReduceDriver(final Reducer<KEYIN, VALUEIN, KEYOUT, Mutation> r) {
        return new TableReduceDriver<>(r);
    }
}
