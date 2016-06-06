package com.flipkart.hbaseobjectmapper.mr.samples;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

import static com.flipkart.hbaseobjectmapper.Util.strToIbw;

public class CitizenMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
    private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Citizen e = hbObjectMapper.readValue(key, value, Citizen.class);
        if (e.getAge() == null)
            return;
        context.write(strToIbw("key"), new IntWritable(e.getAge().intValue()));
    }
}
