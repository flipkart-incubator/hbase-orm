package com.flipkart.hbaseobjectmapper.testcases.mr.samples;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class CitizenMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
    private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Citizen e = hbObjectMapper.readValue(key, value, Citizen.class);
        if (e.getAge() == null)
            return;
        context.write(hbObjectMapper.toIbw("key"), new IntWritable(e.getAge().intValue()));
    }
}
