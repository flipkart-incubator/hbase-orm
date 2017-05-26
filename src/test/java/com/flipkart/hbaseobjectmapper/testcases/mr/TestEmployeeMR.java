package com.flipkart.hbaseobjectmapper.testcases.mr;

import com.flipkart.hbaseobjectmapper.testcases.TestObjects;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;
import com.flipkart.hbaseobjectmapper.testcases.entities.EmployeeSummary;
import com.flipkart.hbaseobjectmapper.testcases.mr.samples.EmployeeMapper;
import com.flipkart.hbaseobjectmapper.testcases.mr.samples.EmployeeReducer;
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

public class TestEmployeeMR extends AbstractMRTest {

    private TableMapDriver<ImmutableBytesWritable, IntWritable> employeeMapDriver;
    private TableReduceDriver<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> employeeReduceDriver;

    @Before
    public void setUp() {
        employeeMapDriver = createMapDriver(new EmployeeMapper());
        employeeReduceDriver = createReduceDriver(new EmployeeReducer());
    }


    @Test
    public void testSingleMapper() throws IOException {
        Employee employee = (Employee) TestObjects.validEmployeeObjectsNoVersion.get(1);
        employeeMapDriver
                .withInput(
                        hbObjectMapper.getRowKey(employee),
                        hbObjectMapper.writeValueAsResult(employee)

                )
                .withOutput(hbObjectMapper.rowKeyToIbw("key"), new IntWritable(employee.getReporteeCount()))
                .runTest();
    }

    @Test
    public void testMultipleMappers() throws IOException {
        List<Pair<ImmutableBytesWritable, Result>> hbRecords = MRTestUtil.writeValueAsRowKeyResultPair(TestObjects.validEmployeeObjectsNoVersion);
        List<Pair<ImmutableBytesWritable, IntWritable>> mapResults = employeeMapDriver.withAll(hbRecords).run();
        for (Pair<ImmutableBytesWritable, IntWritable> mapResult : mapResults) {
            assertEquals("Rowkey got corrupted in Mapper", Bytes.toString(mapResult.getFirst().get()), "key");
        }
    }

    @Test
    public void testReducer() throws Exception {
        Pair<ImmutableBytesWritable, Mutation> reducerResult = employeeReduceDriver.withInput(hbObjectMapper.rowKeyToIbw("key"), Arrays.asList(new IntWritable(1), new IntWritable(5), new IntWritable(0))).run().get(0);
        EmployeeSummary employeeSummary = hbObjectMapper.readValue(reducerResult.getFirst(), (Put) reducerResult.getSecond(), EmployeeSummary.class);
        assertEquals("Unexpected result from EmployeeMapper", (Float) 2.0f, employeeSummary.getAverageReporteeCount());
    }
}
