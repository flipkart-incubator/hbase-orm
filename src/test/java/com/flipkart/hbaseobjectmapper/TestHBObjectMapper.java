package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.*;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.javatuples.Triplet;
import org.junit.Test;

import java.util.*;

import static com.flipkart.hbaseobjectmapper.TestUtil.triplet;
import static org.junit.Assert.*;

public class TestHBObjectMapper {
    @SuppressWarnings("unchecked")
    final List<Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidRecordsAndErrorMessages = Arrays.asList(
            triplet(Singleton.getInstance(), "A singleton class", EmptyConstructorInaccessibleException.class),
            triplet(new ClassWithNoEmptyConstructor(1), "Class with no empty constructor", NoEmptyConstructorException.class),
            triplet(new ClassWithPrimitives(1f), "A class with primitives", MappedColumnCantBePrimitiveException.class),
            triplet(new ClassWithTwoFieldsMappedToSameColumn(), "Class with two fields mapped to same column", FieldsMappedToSameColumnException.class),
            triplet(new ClassWithBadAnnotationStatic(), "Class with a static field mapped to HBase column", MappedColumnCantBeStaticException.class),
            triplet(new ClassWithBadAnnotationTransient("James", "Gosling"), "Class with a transient field mapped to HBase column", MappedColumnCantBeTransientException.class),
            triplet(new ClassWithNoHBColumns(), "Class with no fields mapped with HBColumn", MissingHBColumnFieldsException.class),
            triplet(new ClassWithNoHBRowKeys(), "Class with no fields mapped with HBRowKey", MissingHBRowKeyFieldsException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.NotMap(), "Class with an incompatible field (not Map) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.NotNavigableMap(), "Class with an incompatible field (not NavigableMap) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.EntryKeyNotLong(), "Class with an incompatible field (NavigableMap's entry key not Long) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class)
    );

    final HBObjectMapper hbMapper = new HBObjectMapper();
    final List<Citizen> validObjs = TestObjects.validObjects;

    final Result someResult = hbMapper.writeValueAsResult(validObjs.get(0));
    final Put somePut = hbMapper.writeValueAsPut(validObjs.get(0));

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
        Result result = hbMapper.writeValueAsResult(Arrays.asList(p, p)).get(0);
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
        Put put = hbMapper.writeValueAsPut(Arrays.asList(p, p)).get(0);
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
        Citizen e = TestObjects.validObjects.get(0);
        try {
            hbMapper.readValue("invalid row key", hbMapper.writeValueAsPut(e), Citizen.class);
            fail("Invalid row key should've thrown " + RowKeyCouldNotBeParsedException.class.getName());
        } catch (RowKeyCouldNotBeParsedException ex) {
            System.out.println("For a simulate HBase row with invalid row key, below Exception was thrown as expected:\n" + ex.getMessage() + "\n");
        }
    }

    @Test
    public void testValidClasses() {
        assertTrue(hbMapper.isValid(Citizen.class));
        assertTrue(hbMapper.isValid(CitizenSummary.class));
    }

    @Test
    public void testInvalidClasses() {
        Set<String> exceptionMessages = new HashSet<String>();
        for (Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> p : invalidRecordsAndErrorMessages) {
            HBRecord record = p.getValue0();
            Class recordClass = record.getClass();
            assertFalse("Object mapper couldn't detect invalidity of class " + recordClass.getName(), hbMapper.isValid(recordClass));
            String errorMessage = p.getValue1() + " (" + recordClass.getName() + ") should have thrown an " + IllegalArgumentException.class.getName();
            String exMsgObjToResult = null, exMsgObjToPut = null, exMsgResultToObj = null, exMsgPutToObj = null;
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgObjToResult = ex.getMessage();
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgObjToPut = ex.getMessage();
            }
            try {
                hbMapper.readValue(someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgResultToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(someResult.getRow()), someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
            }
            try {
                hbMapper.readValue(somePut, recordClass);
                fail(errorMessage + " while converting Put to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgPutToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(somePut.getRow()), somePut, recordClass);
                fail(errorMessage + " while converting row key and Put combo to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
            assertEquals("Validation for 'conversion to Result' and 'conversion to Put' differ in code path", exMsgObjToResult, exMsgObjToPut);
            assertEquals("Validation for 'conversion from Result' and 'conversion from Put' differ in code path", exMsgResultToObj, exMsgPutToObj);
            assertEquals("Validation for 'conversion from bean' and 'conversion to bean' differ in code path", exMsgObjToResult, exMsgResultToObj);
            System.out.printf("%s threw below Exception as expected:\n%s\n%n", p.getValue1(), exMsgObjToResult);
            if (!exceptionMessages.add(exMsgObjToPut)) {
                fail("Same error message for different invalid inputs");
            }
        }
    }

    @Test
    public void testInvalidObjs() {
        for (Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> p : TestObjects.invalidObjects) {
            HBRecord record = p.getValue0();
            String errorMessage = "An object with " + p.getValue1() + " should've thrown an " + p.getValue2().getName();
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyResults() {
        Result nullResult = null, blankResult = new Result(), emptyResult = Result.EMPTY_RESULT;
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull("Null Result object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(blankResult, Citizen.class);
        assertNull("Empty Result object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(emptyResult, Citizen.class));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyPuts() {
        Put nullPut = null;
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull("Null Put object should return null", nullCitizen);
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
            hbMapper.getRowKey(new HBRecord() {
                @Override
                public String composeRowKey() {
                    return null;
                }

                @Override
                public void parseRowKey(String rowKey) {

                }
            });
            fail("null row key should've thrown a " + RowKeyCantBeEmptyException.class.getName());
        } catch (RowKeyCantBeEmptyException ignored) {

        }
        try {
            hbMapper.getRowKey(new HBRecord() {
                @Override
                public String composeRowKey() {
                    throw new RuntimeException("Some blah");
                }

                @Override
                public void parseRowKey(String rowKey) {

                }
            });
            fail("If row key can't be composed, an " + RowKeyCantBeComposedException.class.getName() + " was expected");
        } catch (RowKeyCantBeComposedException ignored) {

        }
        try {
            hbMapper.getRowKey(null);
            fail("If object is null, a " + NullPointerException.class.getName() + " was expected");
        } catch (NullPointerException ignored) {

        }
    }

    @Test
    public void testUninstantiatableClass() {
        try {
            hbMapper.readValue(someResult, UninstantiatableClass.class);
            fail("If class can't be instantiated, a " + ObjectNotInstantiatableException.class.getName() + " was expected");
        } catch (ObjectNotInstantiatableException e) {
            // Expected
        }
    }

    @Test
    public void testHBColumnMultiVersion() {
        Double[] testNumbers = new Double[]{3.14159, 2.71828, 0.0};
        for (Double n : testNumbers) {
            // Written as unversioned, read as versioned
            Result result = hbMapper.writeValueAsResult(new CrawlNoVersion("key").setF1(n));
            Crawl versioned = hbMapper.readValue(result, Crawl.class);
            NavigableMap<Long, Double> columnHistory = versioned.getF1();
            assertEquals("Column history size mismatch", 1, columnHistory.size());
            assertEquals(String.format("Inconsistency between %s and %s", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()), n, columnHistory.lastEntry().getValue());
            // Written as versioned, read as unversioned
            Result result1 = hbMapper.writeValueAsResult(new Crawl("key").addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(n));
            CrawlNoVersion unversioned = hbMapper.readValue(result1, CrawlNoVersion.class);
            Double f1 = unversioned.getF1();
            assertEquals(String.format("Inconsistency between %s and %s", HBColumnMultiVersion.class.getSimpleName(), HBColumn.class.getSimpleName()), n, f1);
        }
    }
}
