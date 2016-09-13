package com.flipkart.hbaseobjectmapper.mr.samples;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.entities.EmployeeSummary;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class EmployeeReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
    private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        float averageReporteeCount = (float) sum / (float) count;
        EmployeeSummary employeeSummary = new EmployeeSummary();
        employeeSummary.setAverageReporteeCount(averageReporteeCount);
        context.write(hbObjectMapper.getRowKey(employeeSummary), hbObjectMapper.writeValueAsPut(employeeSummary));
    }
}
