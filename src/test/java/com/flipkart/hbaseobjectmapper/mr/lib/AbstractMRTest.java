package com.flipkart.hbaseobjectmapper.mr.lib;


import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

public class AbstractMRTest {
    protected final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    public void setUp(Configuration conf) {
        conf.setStrings("io.serializations", conf.get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName());
    }
}
