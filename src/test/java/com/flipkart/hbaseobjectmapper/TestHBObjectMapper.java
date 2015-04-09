package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestHBObjectMapper {

    HBObjectMapper hbMapper = new HBObjectMapper();
    List<Citizen> testObjs = TestObjects.citizenList;

    @Test
    public void testHBObjectMapper() {
        for (Citizen obj : testObjs) {
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
        Citizen pFromResult = hbMapper.readValue(result, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result", p, pFromResult);
        System.out.printf("Time taken for Result->POJO = %dms%n%n", end - start);
    }

    public void testResultWithRow(HBRecord p) {
        long start, end;
        Result result = hbMapper.writeValueAsResult(Arrays.asList(p)).get(0);
        ImmutableBytesWritable rowKey = Util.strToIbw(p.composeRowKey());
        start = System.currentTimeMillis();
        Citizen pFromResult = hbMapper.readValue(rowKey, result, Citizen.class);
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
        Citizen pFromPut = hbMapper.readValue(put, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

    public void testPutWithRow(HBRecord p) {
        long start, end;
        Put put = hbMapper.writeValueAsPut(p);
        ImmutableBytesWritable rowKey = Util.strToIbw(p.composeRowKey());
        start = System.currentTimeMillis();
        Citizen pFromPut = hbMapper.readValue(rowKey, put, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

    @Test
    public void testInvalidRow() {
        Citizen e = testObjs.get(0);
        try {
            hbMapper.readValue("invalid row key", hbMapper.writeValueAsPut(e), Citizen.class);
            fail("Invalid row key should've thrown " + IllegalArgumentException.class.getName());
        } catch (IllegalArgumentException ex) {
            System.out.println("Exception was thrown as expected: " + ex.getMessage());
        }
    }

    @Test
    public void testNoEmptyConstructor() {
        NoEmptyConstructor c = new NoEmptyConstructor(0);
        Result result = hbMapper.writeValueAsResult(c);
        try {
            hbMapper.readValue(result, NoEmptyConstructor.class);
            fail("Class without an empty constructor should've thrown an " + IllegalArgumentException.class.getName());
        } catch (IllegalArgumentException ex) {
            System.out.println("Exception was thrown as expected: " + ex.getMessage());

        }
    }

    @Test
    public void testTwoFieldsMappedToSameColumn() {
        TwoFieldsMappedToSameColumn c = new TwoFieldsMappedToSameColumn();
        try {
            hbMapper.writeValueAsResult(c);
            fail("Class with two fields mapped to same column should've thrown an " + IllegalArgumentException.class.getName());
        } catch (IllegalArgumentException ex) {
            System.out.println("Exception was thrown as expected: " + ex.getMessage());
        }
    }

    @Test
    public void testAllFieldsEmpty() {
        AllFieldsEmpty c = new AllFieldsEmpty();
        try {
            hbMapper.writeValueAsResult(c);
            fail("Class with all empty fields should've thrown an " + IllegalArgumentException.class.getName());
        } catch (IllegalArgumentException ex) {
            System.out.println("Exception was thrown as expected: " + ex.getMessage());
        }
    }

    @Test
    public void testNullResults() {
        Result nullResult = null, emptyResult = new Result();
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull("Null Result object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyResult, Citizen.class);
        assertNull("Empty Result object should return null", emptyCitizen);
    }

    @Test
    public void testNullPuts() {
        Put nullPut = null, emptyPut = new Put();
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull("Null Put object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyPut, Citizen.class);
        assertNull("Empty Put object should return null", emptyCitizen);
    }

    @Test
    public void testGetRowKey() {
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(new HBRecord() {
            @Override
            public String composeRowKey() {
                return "rowkey";
            }

            @Override
            public void parseRowKey(String rowKey) {

            }
        });
        assertEquals("Row keys doesn't match", rowKey, Util.strToIbw("rowkey"));
        try {
            ImmutableBytesWritable nullRowKey = hbMapper.getRowKey(new HBRecord() {
                @Override
                public String composeRowKey() {
                    return null;
                }

                @Override
                public void parseRowKey(String rowKey) {

                }
            });
            fail("null row key should've thrown an " + NullPointerException.class.getName());
        } catch (NullPointerException npx) {

        }
        try {
            ImmutableBytesWritable nullRowKey = hbMapper.getRowKey(new HBRecord() {
                @Override
                public String composeRowKey() {
                    throw new RuntimeException("Some blah");
                }

                @Override
                public void parseRowKey(String rowKey) {

                }
            });
            fail("If row key can't be composed, an " + IllegalArgumentException.class.getName() + " was expected");
        } catch (IllegalArgumentException iax) {

        }
        try {
            hbMapper.getRowKey(null);
            fail("If object is null, a " + NullPointerException.class.getName() + " was expected");
        } catch (NullPointerException npx) {

        }
    }
}
