package com.flipkart.hbaseobjectmapper.mr.samples;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.entities.Employee;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class EmployeeMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
    private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Employee e = hbObjectMapper.readValue(key, value, Employee.class);
        if (e.getReporteeCount() != null && e.getReporteeCount() > 0)
            context.write(hbObjectMapper.rowKeyToIbw("key"), new IntWritable(e.getReporteeCount().intValue()));
    }
}
