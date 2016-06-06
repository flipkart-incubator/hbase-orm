package com.flipkart.hbaseobjectmapper.mr;

import com.flipkart.hbaseobjectmapper.Util;
import com.flipkart.hbaseobjectmapper.entities.CitizenSummary;
import com.flipkart.hbaseobjectmapper.mr.lib.AbstractMRTest;
import com.flipkart.hbaseobjectmapper.mr.lib.TableReduceDriver;
import com.flipkart.hbaseobjectmapper.mr.samples.CitizenReducer;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestReducer extends AbstractMRTest {

    private TableReduceDriver<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> reducerDriver;

    @Before
    public void setUp() throws Exception {
        reducerDriver = TableReduceDriver.newTableReduceDriver(new CitizenReducer());
        super.setUp(reducerDriver.getConfiguration());
    }

    @Test
    public void test() throws Exception {
        Pair<ImmutableBytesWritable, Mutation> reducerResult = reducerDriver.withInput(Util.strToIbw("key"), Arrays.asList(new IntWritable(1), new IntWritable(5))).run().get(0);
        CitizenSummary citizenSummary = hbObjectMapper.readValue(reducerResult.getFirst(), (Put) reducerResult.getSecond(), CitizenSummary.class);
        assertEquals(citizenSummary.getAverageAge(), (Float) 3.0f);
    }
}
