package com.flipkart.hbaseobjectmapper.util.mr;


import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

public class AbstractMRTest {
    protected final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    private void configure(Configuration conf) {
        conf.setStrings("io.serializations", conf.get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName());
    }

    public TableMapDriver<ImmutableBytesWritable, IntWritable> createMapDriver(TableMapper<ImmutableBytesWritable, IntWritable> tableMapper) {
        TableMapDriver<ImmutableBytesWritable, IntWritable> mapDriver = TableMapDriver.newTableMapDriver(tableMapper);
        configure(mapDriver.getConfiguration());
        return mapDriver;
    }

    public TableReduceDriver<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> createReduceDriver(TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> tableReducer) {
        TableReduceDriver<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> reduceDriver = TableReduceDriver.newTableReduceDriver(tableReducer);
        configure(reduceDriver.getConfiguration());
        return reduceDriver;
    }
}
