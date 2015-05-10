package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.flipkart.hbaseobjectmapper.TestUtil.pair;
import static org.junit.Assert.*;

public class TestHBObjectMapper {

    HBObjectMapper hbMapper = new HBObjectMapper();
    List<Citizen> validObjs = TestObjects.validObjs;

    @Test
    public void testHBObjectMapper() {
        for (Citizen obj : validObjs) {
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
    public void testInvalidRowKey() {
        Citizen e = TestObjects.validObjs.get(0);
        try {
            hbMapper.readValue("invalid row key", hbMapper.writeValueAsPut(e), Citizen.class);
            fail("Invalid row key should've thrown " + IllegalArgumentException.class.getName());
        } catch (IllegalArgumentException ex) {
            System.out.println("Exception thrown as expected: " + ex.getMessage());
        }
    }

    @Test
    public void testInvalidClasses() {
        List<Pair<HBRecord, String>> invalidRecordsAndErrorMessages = Arrays.asList(
                pair(Singleton.getInstance(), "A singleton class"),
                pair(new ClassWithNoEmptyConstructor(1), "Class with no empty contructor"),
                pair(new ClassWithPrimitives(1f), "A class with primitives"),
                pair(new ClassWithTwoFieldsMappedToSameColumn(), "Class with two fields mapped to same column"),
                pair(new ClassWithUnsupportedDataType(), "Class with a field of unsupported data type"),
                pair(new ClassWithBadAnnotationStatic(), "Class with a static field mapped to HBase column"),
                pair(new ClassWithBadAnnotationTransient("James", "Gosling"), "Class with a transient field mapped to HBase column")
        );
        Set<String> exceptionMessages = new HashSet<String>();
        Result someResult = hbMapper.writeValueAsResult(validObjs.get(0));
        Put somePut = hbMapper.writeValueAsPut(validObjs.get(0));
        for (Pair<HBRecord, String> p : invalidRecordsAndErrorMessages) {
            HBRecord record = p.getFirst();
            Class recordClass = record.getClass();
            assertFalse("Object mapper couldn't detect invalidity of class " + recordClass.getName(), hbMapper.isValid(recordClass));
            String errorMessage = p.getSecond() + " (" + recordClass.getName() + ") should have thrown an " + IllegalArgumentException.class.getName();
            String exMsgObjToResult = null, exMsgObjToPut = null, exMsgResultToObj = null, exMsgPutToObj = null;
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
                exMsgObjToResult = ex.getMessage();
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
                exMsgObjToPut = ex.getMessage();
            }
            try {
                hbMapper.readValue(someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                exMsgResultToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(somePut, recordClass);
                fail(errorMessage + " while converting Put to bean");
            } catch (IllegalArgumentException ex) {
                exMsgPutToObj = ex.getMessage();
            }
            assertEquals("Validation for 'conversion to Result' and 'conversion to Put' differ in code path", exMsgObjToResult, exMsgObjToPut);
            assertEquals("Validation for 'conversion from Result' and 'conversion from Put' differ in code path", exMsgResultToObj, exMsgPutToObj);
            assertEquals("Validation for 'conversion from bean' and 'conversion to bean' differ in code path", exMsgObjToResult, exMsgResultToObj);
            System.out.println("Exception thrown for class " + recordClass.getSimpleName() + " as expected: " + exMsgObjToResult + "\n");
            if (!exceptionMessages.add(exMsgObjToPut)) {
                fail("Same error message for different invalid inputs");
            }
        }
    }

    @Test
    public void testInvalidObjs() {
        for (Pair<HBRecord, String> p : TestObjects.invalidObjs) {
            HBRecord record = p.getFirst();
            String errorMessage = "An object with " + p.getSecond() + " should've thrown an " + IllegalArgumentException.class.getName();
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
            }
        }
    }

    @Test
    public void testEmptyResults() {
        Result nullResult = null, emptyResult = new Result(), resultWithBlankRowKey = new Result(new ImmutableBytesWritable(new byte[]{}));
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull("Null Result object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyResult, Citizen.class);
        assertNull("Empty Result object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(resultWithBlankRowKey, Citizen.class));
    }

    @Test
    public void testEmptyPuts() {
        Put nullPut = null, emptyPut = new Put(), putWithBlankRowKey = new Put(new byte[]{});
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull("Null Put object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyPut, Citizen.class);
        assertNull("Empty Put object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(putWithBlankRowKey, Citizen.class));
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
        assertEquals("Row keys don't match", rowKey, Util.strToIbw("rowkey"));
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
            fail("null row key should've thrown a " + NullPointerException.class.getName());
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
