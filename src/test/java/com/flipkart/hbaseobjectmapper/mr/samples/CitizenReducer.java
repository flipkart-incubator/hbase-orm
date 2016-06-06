package com.flipkart.hbaseobjectmapper.mr.samples;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.entities.CitizenSummary;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class CitizenReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
    private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        float averageAge = (float) sum / (float) count;
        CitizenSummary citizenSummary = new CitizenSummary();
        citizenSummary.setAverageAge(averageAge);
        context.write(hbObjectMapper.getRowKey(citizenSummary), hbObjectMapper.writeValueAsPut(citizenSummary));
    }
}
