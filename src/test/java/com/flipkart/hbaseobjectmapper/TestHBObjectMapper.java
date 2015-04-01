package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.samples.Employee;
import com.flipkart.hbaseobjectmapper.samples.TestObjects;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHBObjectMapper {

    HBObjectMapper hbMapper = new HBObjectMapper();
    List<Employee> testObjs = TestObjects.TEST_OBJECTS;

    @Test
    public void testHBObjectMapper() {
        for (Employee obj : testObjs) {
            System.out.printf("Original object: %s%n", obj);
            testResult(obj);
            testResultWithRow(obj);
            testPut(obj);
            testPutWithRow(obj);
        }
    }

    public void testResult(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Result result = hbMapper.writeValueAsResult(p);
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO->Result = %dms%n", end - start);
        start = System.currentTimeMillis();
        Employee pFromResult = hbMapper.readValue(result, Employee.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result", p, pFromResult);
        System.out.printf("Time taken for Result->POJO = %dms%n%n", end - start);
    }

    public void testResultWithRow(HBRecord p) {
        long start, end;
        Result result = hbMapper.writeValueAsResult(Arrays.asList(p)).get(0);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(p.composeRowKey()));
        start = System.currentTimeMillis();
        Employee pFromResult = hbMapper.readValue(rowKey, result, Employee.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result+Row", p, pFromResult);
        System.out.printf("Time taken for Result+Row->POJO = %dms%n%n", end - start);
    }

    public void testPut(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Put put = hbMapper.writeValueAsPut(Arrays.asList(p)).get(0);
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO->Put = %dms%n", end - start);
        start = System.currentTimeMillis();
        Employee pFromPut = hbMapper.readValue(put, Employee.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

    public void testPutWithRow(HBRecord p) {
        long start, end;
        Put put = hbMapper.writeValueAsPut(p);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(p.composeRowKey()));
        start = System.currentTimeMillis();
        Employee pFromPut = hbMapper.readValue(rowKey, put, Employee.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

}
