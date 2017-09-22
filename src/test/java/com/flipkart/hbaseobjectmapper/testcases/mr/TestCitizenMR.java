package com.flipkart.hbaseobjectmapper.testcases.mr;

import com.flipkart.hbaseobjectmapper.testcases.TestObjects;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import com.flipkart.hbaseobjectmapper.testcases.entities.CitizenSummary;
import com.flipkart.hbaseobjectmapper.testcases.mr.samples.CitizenMapper;
import com.flipkart.hbaseobjectmapper.testcases.mr.samples.CitizenReducer;
import com.flipkart.hbaseobjectmapper.testcases.util.mr.AbstractMRTest;
import com.flipkart.hbaseobjectmapper.testcases.util.mr.MRTestUtil;
import com.flipkart.hbaseobjectmapper.testcases.util.mr.TableMapDriver;
import com.flipkart.hbaseobjectmapper.testcases.util.mr.TableReduceDriver;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestCitizenMR extends AbstractMRTest {

    private TableMapDriver<ImmutableBytesWritable, IntWritable> citizenMapDriver;
    private TableReduceDriver<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> citizenReduceDriver;

    @Before
    public void setUp() {
        citizenMapDriver = createMapDriver(new CitizenMapper());
        citizenReduceDriver = createReduceDriver(new CitizenReducer());
    }


    @Test
    public void testSingleMapper() throws IOException {
        Citizen citizen = TestObjects.validCitizenObjects.get(0);
        citizenMapDriver
                .withInput(
                        hbObjectMapper.getRowKey(citizen),
                        hbObjectMapper.writeValueAsResult(citizen)

                )
                .withOutput(hbObjectMapper.toIbw("key"), new IntWritable(citizen.getAge()))
                .runTest();
    }

    @Test
    public void testMultipleMappers() throws IOException {
        List<Pair<ImmutableBytesWritable, Result>> hbRecords = MRTestUtil.writeValueAsRowKeyResultPair(TestObjects.validCitizenObjects);
        List<Pair<ImmutableBytesWritable, IntWritable>> mapResults = citizenMapDriver.withAll(hbRecords).run();
        for (Pair<ImmutableBytesWritable, IntWritable> mapResult : mapResults) {
            assertEquals("Rowkey got corrupted in Mapper", Bytes.toString(mapResult.getFirst().get()), "key");
        }
    }

    @Test
    public void testReducer() throws Exception {
        Pair<ImmutableBytesWritable, Mutation> reducerResult = citizenReduceDriver.withInput(hbObjectMapper.toIbw("key"), Arrays.asList(new IntWritable(1), new IntWritable(5))).run().get(0);
        CitizenSummary citizenSummary = hbObjectMapper.readValue(reducerResult.getFirst(), (Put) reducerResult.getSecond(), CitizenSummary.class);
        assertEquals("Unexpected result from CitizenReducer", (Float) 3.0f, citizenSummary.getAverageAge());
    }
}
