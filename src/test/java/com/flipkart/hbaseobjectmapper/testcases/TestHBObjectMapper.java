package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import com.flipkart.hbaseobjectmapper.testcases.entities.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.*;

import static com.flipkart.hbaseobjectmapper.testcases.TestObjects.validObjects;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
public class TestHBObjectMapper {
    public static final int NUM_ITERATIONS = 100;
    @SuppressWarnings("unchecked")
    private final List<Triple<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidRecordsAndErrorMessages = Arrays.asList(
            triple(Singleton.getInstance(), "A singleton class", EmptyConstructorInaccessibleException.class),
            triple(new ClassWithNoEmptyConstructor(1), "Class with no empty constructor", NoEmptyConstructorException.class),
            triple(new ClassWithPrimitives(1f), "A class with primitives", MappedColumnCantBePrimitiveException.class),
            triple(new ClassWithTwoFieldsMappedToSameColumn(), "Class with two fields mapped to same column", FieldsMappedToSameColumnException.class),
            triple(new ClassWithBadAnnotationStatic(), "Class with a static field mapped to HBase column", MappedColumnCantBeStaticException.class),
            triple(new ClassWithBadAnnotationTransient("James", "Gosling"), "Class with a transient field mapped to HBase column", MappedColumnCantBeTransientException.class),
            triple(new ClassWithNoHBColumns(), "Class with no fields mapped with HBColumn", MissingHBColumnFieldsException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.NotMap(), "Class with an incompatible field (not Map) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.NotNavigableMap(), "Class with an incompatible field (not NavigableMap) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.EntryKeyNotLong(), "Class with an incompatible field (NavigableMap's entry key not Long) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.InvalidVersions(), "Class with an invalid number of versions in it's HBTable annotation", ImproperHBTableAnnotationExceptions.InvalidValueForVersionsOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.EmptyTableName(), "Class with empty table name in it's HBTable annotation", ImproperHBTableAnnotationExceptions.EmptyTableNameOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.EmptyColumnFamily(), "Class with empty column family name in it's HBTable annotation", ImproperHBTableAnnotationExceptions.EmptyColumnFamilyOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.DuplicateColumnFamilies(), "Class with duplicate column families in it's HBTable annotation", ImproperHBTableAnnotationExceptions.DuplicateColumnFamilyNamesOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.MissingHBTableAnnotation(), "Class with no HBTable annotation", ImproperHBTableAnnotationExceptions.MissingHBTableAnnotationException.class)
    );

    private static Triple<HBRecord, String, Class<? extends IllegalArgumentException>> triple(HBRecord record, String message, Class<? extends IllegalArgumentException> exceptionClass) {
        return Triple.create(record, message, exceptionClass);
    }

    final HBObjectMapper hbMapper = new HBObjectMapper();

    final Result someResult = hbMapper.writeValueAsResult(validObjects.get(0));
    final Put somePut = hbMapper.writeValueAsPut(validObjects.get(0));

    @Test
    public void testHBObjectMapper() {
        for (HBRecord obj : validObjects) {
            System.out.printf("Original object: %s%n", obj);
            testResult(obj);
            testResultWithRow(obj);
            testPut(obj);
            testPutWithRow(obj);
            System.out.printf("*****%n%n");
        }
    }

    private void testResult(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Result result = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            result = hbMapper.writeValueAsResult(p);
        }
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO -> Result = %.2fms%n", timeTaken(start, end));
        start = System.currentTimeMillis();
        HBRecord pFromResult = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromResult = hbMapper.readValue(result, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals(p, pFromResult, "Data mismatch after deserialization from Result");
        System.out.printf("Time taken for Result -> POJO = %.2fms%n", timeTaken(start, end));
    }

    private double timeTaken(long start, long end) {
        return (double) (end - start) / (double) NUM_ITERATIONS;
    }

    private <R extends Serializable & Comparable<R>> void testResultWithRow(HBRecord<R> p) {
        long start, end;
        Result result = hbMapper.writeValueAsResult(l(p, p)).get(0);
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(p);
        start = System.currentTimeMillis();
        HBRecord pFromResult = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromResult = hbMapper.readValue(rowKey, result, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals(p, pFromResult, "Data mismatch after deserialization from Result+Row");
        System.out.printf("Time taken for Rowkey+Result -> POJO = %.2fms%n", timeTaken(start, end));
    }

    @SafeVarargs
    private final <R extends Serializable & Comparable<R>> List<HBRecord<R>> l(HBRecord<R>... records) {
        ArrayList<HBRecord<R>> list = new ArrayList<>();
        Collections.addAll(list, records);
        return list;

    }

    private void testPut(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Put put = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            put = (Put) hbMapper.writeValueAsPut(l(p, p)).get(0);
        }
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO -> Put = %.2fms%n", timeTaken(start, end));
        start = System.currentTimeMillis();
        HBRecord pFromPut = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromPut = hbMapper.readValue(put, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals(p, pFromPut, "Data mismatch after deserialization from Put");
        System.out.printf("Time taken for Put -> POJO = %.2fms%n", timeTaken(start, end));
    }

    private <R extends Serializable & Comparable<R>> void testPutWithRow(HBRecord<R> p) {
        long start, end;
        Put put = hbMapper.writeValueAsPut(p);
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(p);
        start = System.currentTimeMillis();
        HBRecord pFromPut = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromPut = hbMapper.readValue(rowKey, put, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals(p, pFromPut, "Data mismatch after deserialization from Put");
        System.out.printf("Time taken for Rowkey+Put -> POJO = %.2fms%n%n", timeTaken(start, end));
    }

    @Test
    public void testInvalidRowKey() {
        assertThrows(RowKeyCouldNotBeParsedException.class, () -> hbMapper.readValue(hbMapper.toIbw("invalid row key"), hbMapper.writeValueAsPut(validObjects.get(0)), Citizen.class));
    }

    @Test
    public void testValidClasses() {
        for (Class clazz : Arrays.asList(Citizen.class, CitizenSummary.class, Employee.class, EmployeeSummary.class)) {
            assertTrue(hbMapper.isValid(clazz));
        }
    }

    @Test
    public void testInvalidClasses() {
        final String ERROR_MESSAGE = "Mismatch in type of exception thrown for class ";
        Set<String> exceptionMessages = new HashSet<>();
        for (Triple<HBRecord, String, Class<? extends IllegalArgumentException>> p : invalidRecordsAndErrorMessages) {
            HBRecord record = p.getFirst();
            Class recordClass = record.getClass();
            assertFalse(hbMapper.isValid(recordClass), "Object mapper couldn't detect issue with invalid class " + recordClass.getName());
            String errorMessage = p.getSecond() + " (" + recordClass.getName() + ") should have thrown an " + IllegalArgumentException.class.getName();
            String exMsgObjToResult = null, exMsgObjToPut = null, exMsgResultToObj = null, exMsgPutToObj = null;
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
                exMsgObjToResult = ex.getMessage();
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
                exMsgObjToPut = ex.getMessage();
            }
            try {
                hbMapper.readValue(someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
                exMsgResultToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(someResult.getRow()), someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
            }
            try {
                hbMapper.readValue(somePut, recordClass);
                fail(errorMessage + " while converting Put to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
                exMsgPutToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(somePut.getRow()), somePut, recordClass);
                fail(errorMessage + " while converting row key and Put combo to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), ERROR_MESSAGE + recordClass.getSimpleName());
            }
            assertEquals(exMsgObjToResult, exMsgObjToPut, "Validation for 'conversion to Result' and 'conversion to Put' differ in code path");
            assertEquals(exMsgResultToObj, exMsgPutToObj, "Validation for 'conversion from Result' and 'conversion from Put' differ in code path");
            assertEquals(exMsgObjToResult, exMsgResultToObj, "Validation for 'conversion from bean' and 'conversion to bean' differ in code path");
            System.out.printf("[edge case] %s threw below Exception, as expected:%n%s%n%n", p.getSecond(), exMsgObjToResult);
            assertTrue(exceptionMessages.add(exMsgObjToPut), "Same error message for different invalid inputs");
        }
    }

    @Test
    public void testInvalidObjects() {
        for (Triple<HBRecord, String, Class<? extends IllegalArgumentException>> p : TestObjects.invalidObjects) {
            HBRecord record = p.getFirst();
            String errorMessage = "An object with " + p.getSecond() + " should've thrown an " + p.getThird().getName();
            try {
                hbMapper.writeValueAsResult(record);
                fail(String.format("%s while converting bean to Result%nFailing object = %s", errorMessage, record));
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), "Mismatch in type of exception thrown");
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(String.format("%s while converting bean to Put%nFailing object = %s", errorMessage, record));
            } catch (IllegalArgumentException ex) {
                assertEquals(p.getThird(), ex.getClass(), "Mismatch in type of exception thrown");
            }
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyResults() {
        Result nullResult = null, blankResult = new Result(), emptyResult = Result.EMPTY_RESULT;
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull(nullCitizen, "Null Result object should return null");
        Citizen emptyCitizen = hbMapper.readValue(blankResult, Citizen.class);
        assertNull(emptyCitizen, "Empty Result object should return null");
        assertNull(hbMapper.readValue(emptyResult, Citizen.class));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyPuts() {
        Put nullPut = null;
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull(nullCitizen, "Null Put object should return null");
    }

    @HBTable(name = "dummy1", families = {@Family(name = "a")})
    public static class DummyRowKeyClass implements HBRecord<String> {
        private String rowKey;

        @HBColumn(family = "a", column = "d")
        private String dummy;

        public DummyRowKeyClass() {

        }

        public DummyRowKeyClass(String rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public String composeRowKey() {
            return rowKey;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.rowKey = rowKey;
        }
    }

    @HBTable(name = "dummy2", families = {@Family(name = "a")})
    public static class RowKeyComposeThrowsExceptionClass implements HBRecord<String> {
        private String rowKey;

        @HBColumn(family = "a", column = "d")
        private String dummy;

        @Override
        public String composeRowKey() {
            throw new RuntimeException("Some blah");
        }

        @Override
        public void parseRowKey(String rowKey) {

        }
    }

    @Test
    public void testGetRowKey() {
        assertEquals(hbMapper.getRowKey(new DummyRowKeyClass("rowkey")), hbMapper.toIbw("rowkey"), "Row keys don't match");
        try {
            hbMapper.getRowKey(new DummyRowKeyClass(null));
            fail("null row key should've thrown a " + RowKeyCantBeEmptyException.class.getName());
        } catch (RowKeyCantBeEmptyException ignored) {

        }
        try {
            hbMapper.getRowKey(new RowKeyComposeThrowsExceptionClass());
            fail("If row key can't be composed, an " + RowKeyCantBeComposedException.class.getName() + " was expected");
        } catch (RowKeyCantBeComposedException ignored) {

        }
        try {
            HBRecord<Integer> nullRecord = null;
            hbMapper.getRowKey(nullRecord);
            fail("If object is null, a " + NullPointerException.class.getSimpleName() + " was expected");
        } catch (NullPointerException ignored) {

        }
    }

    @Test
    public void testUninstantiatableClass() {
        try {
            hbMapper.readValue(someResult, UninstantiatableClass.class);
            fail("If class can't be instantiated, a " + ObjectNotInstantiatableException.class.getName() + " was expected");
        } catch (ObjectNotInstantiatableException ignored) {
        }
    }

    @Test
    public void testHBColumnMultiVersion() {
        Double[] numbers = new Double[]{0.0, 3.14159, 2.71828};
        for (Double number : numbers) {
            // Written as unversioned, read as versioned
            Result result = hbMapper.writeValueAsResult(new CrawlNoVersion("key").setF1(number));
            Crawl versioned = hbMapper.readValue(result, Crawl.class);
            NavigableMap<Long, Double> columnHistory = versioned.getF1Versioned();
            assertEquals(1, columnHistory.size(), "Column history size mismatch");
            assertEquals(number, columnHistory.lastEntry().getValue(), String.format("Inconsistency between %s and %s",
                    HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()));
            // Written as versioned, read as unversioned
            Crawl crawl = new Crawl("key").addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE);
            Crawl versionedCrawl = crawl.addF1(number);
            Result crawlAsResult = hbMapper.writeValueAsResult(versionedCrawl);
            CrawlNoVersion unversionedCrawl = hbMapper.readValue(crawlAsResult, CrawlNoVersion.class);
            assertEquals(number, unversionedCrawl.getF1(), String.format("Inconsistency between %s and %s%n" +
                            "Versioned (persisted) object = %s%nUnversioned (retrieved) object = %s",
                    HBColumnMultiVersion.class.getSimpleName(), HBColumn.class.getSimpleName(),
                    versionedCrawl, unversionedCrawl));
        }
    }
}
