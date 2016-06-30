package com.flipkart.hbaseobjectmapper.mr;

import com.flipkart.hbaseobjectmapper.TestObjects;
import com.flipkart.hbaseobjectmapper.TestUtil;
import com.flipkart.hbaseobjectmapper.Util;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.flipkart.hbaseobjectmapper.mr.lib.AbstractMRTest;
import com.flipkart.hbaseobjectmapper.mr.lib.TableMapDriver;
import com.flipkart.hbaseobjectmapper.mr.samples.CitizenMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMapper extends AbstractMRTest {

    private TableMapDriver<ImmutableBytesWritable, IntWritable> mapDriver;

    @Before
    public void setUp() {
        mapDriver = TableMapDriver.newTableMapDriver(new CitizenMapper());
        super.setUp(mapDriver.getConfiguration());
    }

    @Test
    public void testSingle() throws IOException {
        Citizen citizen = TestObjects.validObjects.get(0);
        org.apache.hadoop.hbase.util.Pair<ImmutableBytesWritable, Result> rowKeyResultPair = hbObjectMapper.writeValueAsRowKeyResultPair(citizen);
        mapDriver
                .withInput(
                        rowKeyResultPair.getFirst(), // this line can alternatively be hbObjectMapper.getRowKey(citizen)
                        rowKeyResultPair.getSecond() // this line can alternatively be hbObjectMapper.writeValueAsResult(citizen)

                )
                .withOutput(Util.strToIbw("key"), new IntWritable(citizen.getAge()))
                .runTest();
    }


    @Test
    public void testMultiple() throws IOException {
        List<Pair<ImmutableBytesWritable, Result>> citizens = TestUtil.writeValueAsRowKeyResultPair(TestObjects.validObjects);
        List<Pair<ImmutableBytesWritable, IntWritable>> mapResults = mapDriver.withAll(citizens).run();
        for (Pair<ImmutableBytesWritable, IntWritable> mapResult : mapResults) {
            assertEquals(Util.ibwToStr(mapResult.getFirst()), "key");
        }
    }
}
