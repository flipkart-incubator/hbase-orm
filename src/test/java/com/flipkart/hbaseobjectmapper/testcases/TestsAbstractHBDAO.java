package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.codec.JavaObjectStreamCodec;
import com.flipkart.hbaseobjectmapper.testcases.daos.*;
import com.flipkart.hbaseobjectmapper.testcases.entities.*;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.*;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;

import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
public class TestsAbstractHBDAO {
    private static Connection connection;
    private static HBAdmin hbAdmin;
    private static HBaseCluster hBaseCluster;

    @BeforeAll
    public static void setup() {
        try {
            Logger.getRootLogger().setLevel(Level.WARN);
            String useRealHBase = System.getenv(RealHBaseCluster.USE_REAL_HBASE);
            if (useRealHBase != null && (useRealHBase.equals("1") || useRealHBase.equalsIgnoreCase("true"))) {
                hBaseCluster = new RealHBaseCluster();
            } else {
                String inMemoryHBaseClusterStartTimeout = System.getenv(InMemoryHBaseCluster.INMEMORY_CLUSTER_START_TIMEOUT);
                if (inMemoryHBaseClusterStartTimeout != null) {
                    hBaseCluster = new InMemoryHBaseCluster(Long.parseLong(inMemoryHBaseClusterStartTimeout));
                } else {
                    hBaseCluster = new InMemoryHBaseCluster();
                }
            }
            connection = hBaseCluster.start();
            hbAdmin = new HBAdmin(connection);
        } catch (NumberFormatException e) {
            fail("The environmental variable " + InMemoryHBaseCluster.INMEMORY_CLUSTER_START_TIMEOUT + " is specified incorrectly (Must be numeric)");
        } catch (Exception e) {
            e.printStackTrace(System.err);
            fail(String.format("Failed to connect to HBase. Aborted execution of DAO-related test cases." +
                    "Reason:%n%s", e.getMessage()));
        }
    }

    private static <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTables(Class... classes) throws IOException {
        for (Class<T> clazz : classes) {
            WrappedHBTableTC<R, T> hbTable = new WrappedHBTableTC<>(clazz);
            if (hbAdmin.tableExists(clazz)) {
                System.out.format("Deleting table '%s': ", hbTable);
                hbAdmin.disableTable(clazz);
                hbAdmin.deleteTable(clazz);
                System.out.println("[DONE]");
            }
        }
    }

    private static <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTables(Class... classes) throws IOException {
        for (Class<T> clazz : classes) {
            WrappedHBTableTC<R, T> hbTable = new WrappedHBTableTC<>(clazz);
            System.out.format("Creating table '%s': ", hbTable);
            hbAdmin.createTable(clazz);
            System.out.println("[DONE]");
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T pruneVersionsBeyond(T record, int versions) {
        try {
            T prunedRecord = JavaObjectStreamCodec.deepCopy(record);
            for (Field field : Citizen.class.getDeclaredFields()) {
                WrappedHBColumnTC hbColumn = new WrappedHBColumnTC(field);
                if (hbColumn.isMultiVersioned()) {
                    field.setAccessible(true);
                    NavigableMap nm = (NavigableMap) field.get(prunedRecord);
                    if (nm == null || nm.isEmpty())
                        continue;
                    NavigableMap temp = new TreeMap<>();
                    for (int i = 0; i < versions; i++) {
                        final Map.Entry entry = nm.pollLastEntry();
                        if (entry == null)
                            break;
                        temp.put(entry.getKey(), entry.getValue());
                    }
                    field.set(prunedRecord, temp.isEmpty() ? null : temp);
                }
            }
            return prunedRecord;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCRUD() throws IOException {
        try {
            createTables(Citizen.class, CitizenSummary.class);
            CitizenDAO citizenDao = new CitizenDAO(connection);
            CitizenSummaryDAO citizenSummaryDAO = new CitizenSummaryDAO(connection);
            final List<Citizen> records = TestObjects.validCitizenObjects;
            assertEquals("citizens", citizenDao.getTableName());
            final Set<String> columnFamiliesCitizen = citizenDao.getColumnFamiliesAndVersions().keySet(), columnFamiliesCitizenSummary = citizenSummaryDAO.getColumnFamiliesAndVersions().keySet();
            assertEquals(s("main", "optional"), columnFamiliesCitizen, "Issue with column families of 'citizens' table%n" + columnFamiliesCitizen);
            assertEquals("citizens_summary", citizenSummaryDAO.getTableName());
            assertEquals(s("a"), columnFamiliesCitizenSummary, "Issue with column families of 'citizens_summary' table%n" + columnFamiliesCitizenSummary);
            String[] allRowKeys = new String[records.size()];
            Map<String, Map<String, Object>> expectedFieldValues = new HashMap<>();
            for (int i = 0; i < records.size(); i++) { // for each test object,
                Citizen record = records.get(i);
                final String rowKey = citizenDao.persist(record);
                allRowKeys[i] = rowKey;
                Citizen serDeserRecord = citizenDao.get(rowKey, Integer.MAX_VALUE);
                assertEquals(record, serDeserRecord, "Entry got corrupted upon persisting and fetching back");
                for (int numVersions = 1; numVersions <= 4; numVersions++) {
                    final Citizen citizenNVersionsActual = citizenDao.get(rowKey, numVersions), citizenNVersionsExpected = pruneVersionsBeyond(record, numVersions);
                    assertEquals(citizenNVersionsExpected, citizenNVersionsActual, String.format("Mismatch in data between 'record pruned for %d versions' and 'record fetched from HBase for %d versions' for record: %s", numVersions, numVersions, record));
                }
                for (String f : citizenDao.getFields()) { // for each field of the given test object,
                    try {
                        Field field = Citizen.class.getDeclaredField(f);
                        WrappedHBColumnTC hbColumn = new WrappedHBColumnTC(field);
                        field.setAccessible(true);
                        if (hbColumn.isMultiVersioned()) {
                            NavigableMap expected = (NavigableMap) field.get(record);
                            final NavigableMap actual = citizenDao.fetchFieldValue(rowKey, f, Integer.MAX_VALUE);
                            assertEquals(expected, actual, String.format("Data for (multi-versioned) field \"%s\" got corrupted upon persisting and fetching back object: %s", field.getName(), record));
                            if (actual == null)
                                continue;
                            if (expectedFieldValues.containsKey(f)) {
                                expectedFieldValues.get(f).put(rowKey, actual);
                            } else {
                                expectedFieldValues.put(f, m(e(rowKey, (Object) actual)));
                            }
                        } else {
                            final Object actual = citizenDao.fetchFieldValue(rowKey, f);
                            Object expected = field.get(record);
                            assertEquals(expected, actual, String.format("Data for field \"%s\" got corrupted upon persisting and fetching back object: %s", field.getName(), record));
                            if (actual == null)
                                continue;
                            if (expectedFieldValues.containsKey(f)) {
                                expectedFieldValues.get(f).put(rowKey, actual);
                            } else {
                                expectedFieldValues.put(f, m(e(rowKey, actual)));
                            }
                        }
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        fail(String.format("Can't read field '%s' from object %s", f, record));
                    } catch (NoSuchFieldException e) {
                        e.printStackTrace();
                        fail(String.format("Field missing: %s", f));
                    } catch (IOException ioex) {
                        ioex.printStackTrace();
                        fail(String.format("Could not fetch field '%s' for row '%s'", f, rowKey));
                    }
                }
            }
            // Test on range scan
            final String startRowKey = allRowKeys[0], endRowKey = allRowKeys[allRowKeys.length - 1];
            List<Citizen> citizens = citizenDao.get(startRowKey, endRowKey, Integer.MAX_VALUE);
            for (int i = 0; i < citizens.size(); i++) {
                assertEquals(records.get(i), citizens.get(i), String.format("[range scan] The result of get(%s, %s) returned unexpected entry at position " + i, startRowKey, endRowKey));
            }
            try (Records<Citizen> citizenIterable = citizenDao.records(startRowKey, endRowKey, Integer.MAX_VALUE)) {
                Citizen[] expectedCitizens = citizens.toArray(new Citizen[0]); // this contains all records except the last one
                Citizen[] actualCitizens = Iterables.toArray(citizenIterable, Citizen.class);
                assertArrayEquals(expectedCitizens, actualCitizens, "Fetch directly vs fetch via iterable differ in results [start row key, end row key]");
            }
            Citizen[] allCitizens = citizenDao.get(Arrays.asList(allRowKeys)).toArray(new Citizen[allRowKeys.length]);
            List<Citizen> citizensByPrefix = citizenDao.getByPrefix(citizenDao.toBytes("IND#"));
            assertArrayEquals(citizensByPrefix.toArray(new Citizen[0]), allCitizens, "get by prefix is returning incorrect result");
            try (Records<Citizen> citizenIterable = citizenDao.recordsByPrefix(citizenDao.toBytes("IND#"))) {
                Citizen[] expectedCitizens = citizenDao.get(allRowKeys);
                assertArrayEquals(expectedCitizens, Iterables.toArray(citizenIterable, Citizen.class), "Fetch directly vs fetch via iterable differ in results [row key prefix]");
                assertArrayEquals(expectedCitizens, allCitizens, "Results of Get by array of row keys did not match that of list");
            }
            try (Records<Citizen> citizenIterable = citizenDao.records("IND#101", true, "IND#102", true, 1, 1000)) {
                Iterator<Citizen> iterator = citizenIterable.iterator();
                Citizen citizen1 = iterator.next();
                Citizen citizen2 = iterator.next();
                assertEquals(citizenDao.get("IND#101"), citizen1, "Get by iterable didn't match get by individual record");
                assertEquals(citizenDao.get("IND#102"), citizen2, "Get by iterable didn't match get by individual record");
            }

            // Range Get vs Bulk Get (Single-version)
            for (String f : citizenDao.getFields()) {
                Map<String, Object> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f);
                assertEquals(fieldValuesBulkGetFull, fieldValuesRangeGetFull, "[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for full range");
                Map<String, Object> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#104", "IND#105", "IND#106"), f),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#104", "IND#107", f);
                assertEquals(fieldValuesBulkGetPartial, fieldValuesRangeGetPartial, "[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for partial range");
            }

            // Range Get vs Bulk Get (Multi-version)
            for (String f : citizenDao.getFields()) {
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f, Integer.MAX_VALUE),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f, Integer.MAX_VALUE);
                assertEquals(fieldValuesBulkGetFull, fieldValuesRangeGetFull, "[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for full range");
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#101", "IND#102", "IND#103"), f, Integer.MAX_VALUE),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#101", "IND#104", f, Integer.MAX_VALUE);
                assertEquals(fieldValuesBulkGetPartial, fieldValuesRangeGetPartial, "[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for partial range");
            }

            // Test for a single field (redundant test, but that's ok):
            Map<String, Object> actualSalaries = citizenDao.fetchFieldValues(allRowKeys, "sal");
            long actualSumOfSalaries = 0;
            for (Object s : actualSalaries.values()) {
                actualSumOfSalaries += s == null ? 0 : (Integer) s;
            }
            long expectedSumOfSalaries = 0;
            for (Citizen c : records) {
                expectedSumOfSalaries += c.getSal() == null ? 0 : c.getSal();
            }
            assertEquals(expectedSumOfSalaries, actualSumOfSalaries);

            // Test record deletion:
            Citizen citizenToBeDeleted = records.get(0);
            citizenDao.delete(citizenToBeDeleted);
            assertNull(citizenDao.get(citizenToBeDeleted.composeRowKey()), "Record was not deleted: " + citizenToBeDeleted);
            List<Citizen> citizensToBeDeleted = Arrays.asList(records.get(1), records.get(2));
            citizenDao.delete(citizensToBeDeleted);
            assertNull(citizenDao.get(citizensToBeDeleted.get(0).composeRowKey()), "Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(0));
            assertNull(citizenDao.get(citizensToBeDeleted.get(1).composeRowKey()), "Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(1));
            final String rowKey3 = records.get(3).composeRowKey(), rowKey4 = records.get(4).composeRowKey();
            citizenDao.delete(new String[]{rowKey3, rowKey4});
            assertNull(citizenDao.get(rowKey3), "Record was not deleted when deleted by 'array of row keys': " + rowKey3);
            assertNull(citizenDao.get(rowKey4), "Record was not deleted when deleted by 'array of row keys': " + rowKey4);
        } finally {
            deleteTables(Citizen.class, CitizenSummary.class);
        }
    }

    @Test
    public void testAppend() throws IOException {
        try {
            createTables(Citizen.class);
            CitizenDAO citizenDao = new CitizenDAO(connection);
            Citizen citizenBeforeAppend = new Citizen("IND", 120, "Abdul", null, null, null, null, null, null, null, null, null, null, new Dependents(null, Arrays.asList(141, 142)), null);
            assertNull(citizenBeforeAppend.getSal());
            String rowKey = citizenDao.persist(citizenBeforeAppend);
            Integer expectedSalary = 30000;
            citizenDao.append(rowKey, "sal", expectedSalary);
            try {
                citizenDao.append(rowKey, "blahblah", 5);
                fail("An attempt was made to append value a non-existent field. This should have thrown an exception - It didn't.");
            } catch (Exception e) {
                System.out.printf("[egde case] Got error as expected, for non-existent column: %s%n", e.getMessage());
            }
            Citizen citizenAfter1Append = citizenDao.get(rowKey);
            assertEquals(expectedSalary, citizenAfter1Append.getSal(), "Append operation didn't work as expected on field 'sal'");
            List<Contact> expectedContacts = l(new Contact("contact1", 23411));
            citizenDao.append(rowKey, "emergencyContacts1", expectedContacts);
            Citizen citizenAfter2Append = citizenDao.get(rowKey);
            assertEquals(expectedContacts, citizenAfter2Append.getEmergencyContacts1(), "Append operation didn't work as expected on field 'emergencyContacts1'");
            try {
                citizenDao.append(rowKey, m(e("f3", 123L), e("f4", "blah blah blah")));
                fail("An attempt was made to append a BigDecimal field with a String value - This should have thrown an exception - It didn't.");
            } catch (Exception e) {
                System.out.printf("[edge case] Got error as expected, for type mismatch in columns: %s%n", e.getMessage());
            }
            Citizen citizenAfter3Append = citizenDao.get(rowKey);
            assertNull(citizenAfter3Append.getF3(), "Append operation broke 'all or none' semantics");
            citizenDao.append(rowKey, m(e("f3", 123L)));
            Citizen citizenAfter4Append = citizenDao.get(rowKey);
            assertEquals(123L, (long) citizenAfter4Append.getF3(), "Append operation failed for f3");
            citizenDao.append(rowKey, "name", " Kalam");
            assertEquals("Abdul Kalam", citizenDao.fetchFieldValue(rowKey, "name"), "Append operation failed for name");
        } finally {
            deleteTables(Citizen.class);
        }
    }

    @Test
    public void testCustom() throws IOException {
        try {
            createTables(Counter.class);
            CounterDAO counterDAO = new CounterDAO(connection);
            Counter counter = new Counter("c1");
            for (int i = 1; i <= 10; i++) {
                counter.setValue((long) i, (long) i);
            }
            counter.setVar(0L);
            final String rowKey = counterDAO.persist(counter);
            // Test custom timestamp values:
            assertEquals(counterDAO.get(rowKey, 7), counterDAO.getOnGet(counterDAO.getGet(rowKey).readVersions(7)), "Unexpected values on get (number of versions)");
            assertEquals(nm(e(10L, 10L)), counterDAO.getOnGet(counterDAO.getGet(rowKey).setTimestamp(10)).getValue(), "Unexpected values on get (given timestamp)");
            assertEquals(Arrays.asList(new Counter("c1", nm(e(1L, 1L), e(2L, 2L), e(3L, 3L), e(4L, 4L))), new Counter("c1", nm(e(3L, 3L), e(4L, 4L)))),
                    counterDAO.getOnGets(Arrays.asList(counterDAO.getGet(rowKey).setTimeRange(1, 5).readAllVersions(), counterDAO.getGet(rowKey).setTimeRange(1, 5).readVersions(2))),
                    "Unexpected values on bulk get");
            // Test increment features:
            assertEquals(1L, counterDAO.increment(rowKey, "var", 1L), "Increment didn't apply - basic");
            assertEquals(1L, (long) counterDAO.fetchFieldValue(rowKey, "var"), "Increment apply didn't reflect in fetch field - basic");
            assertEquals(3L, counterDAO.increment(rowKey, "var", 2L, Durability.SKIP_WAL), "Increment didn't apply - with durability flag");
            assertEquals(3L, counterDAO.fetchFieldValue(rowKey, "var"), "Increment apply didn't reflect in fetch field - with durability flag");
            Increment increment = counterDAO.getIncrement(rowKey).addColumn(Bytes.toBytes("a"), Bytes.toBytes("var"), 5L);
            Counter persistedCounter = counterDAO.increment(increment);
            assertEquals(8L, persistedCounter.getVar(), "Increment didn't reflect in object get - native way");
            assertEquals(8L, (long) counterDAO.fetchFieldValue(rowKey, "var"), "Increment didn't apply in fetch field - native way");
            try {
                counterDAO.increment(rowKey, "badvarI", 4L);
                fail("Attempt to increment a field that isn't Long succeeded (it shouldn't have)");
            } catch (Exception ignored) {
                //nothing
            }
        } finally {
            deleteTables(Counter.class);
        }
    }

    @Test
    public void testVersioning() throws IOException {
        try {
            createTables(Crawl.class);
            CrawlDAO crawlDAO = new CrawlDAO(connection);
            CrawlNoVersionDAO crawlNoVersionDAO = new CrawlNoVersionDAO(connection);
            final int NUM_VERSIONS = 3;
            Double[] testNumbers = new Double[]{-1.0, Double.MAX_VALUE, Double.MIN_VALUE, 3.14159, 2.71828, 1.0};
            Double[] testNumbersOfRange = Arrays.copyOfRange(testNumbers, testNumbers.length - NUM_VERSIONS, testNumbers.length);
            // Written as unversioned, read as versioned
            List<CrawlNoVersion> objs = new ArrayList<>();
            for (Double n : testNumbers) {
                objs.add(new CrawlNoVersion("key").setF1(n));
            }
            crawlNoVersionDAO.persist(objs);
            Crawl crawl = crawlDAO.get("key", NUM_VERSIONS);
            assertEquals(1.0, crawl.getF1().values().iterator().next(), 1e-9, "Issue with version history implementation when written as unversioned and read as versioned");
            crawlDAO.delete("key");
            Crawl versioned = crawlDAO.get("key");
            assertNull(versioned, "Deleted row (with key " + versioned + ") still exists when accessed as versioned DAO");
            CrawlNoVersion versionless = crawlNoVersionDAO.get("key");
            assertNull(versionless, "Deleted row (with key " + versionless + ") still exists when accessed as versionless DAO");
            // Written as versioned, read as unversioned+versioned
            Crawl crawl2 = new Crawl("key2");
            long timestamp = System.currentTimeMillis();
            long i = 0;
            for (Double n : testNumbers) {
                crawl2.addF1(timestamp + i, n);
                i++;
            }
            crawlDAO.persist(crawl2);
            CrawlNoVersion crawlNoVersion = crawlNoVersionDAO.get("key2");
            assertEquals(crawlNoVersion.getF1(), testNumbers[testNumbers.length - 1], "Entry with the highest version (i.e. timestamp) isn't the one that was returned by DAO get");
            assertArrayEquals(testNumbersOfRange, crawlDAO.get("key2", NUM_VERSIONS).getF1().values().toArray(), "Issue with version history implementation when written as versioned and read as unversioned");

            List<String> rowKeysList = new ArrayList<>();
            for (int v = 0; v <= 9; v++) {
                for (int k = 1; k <= 4; k++) {
                    String key = "oKey" + k;
                    crawlDAO.persist(new Crawl(key).addF1((double) v));
                    rowKeysList.add(key);
                }
            }
            String[] rowKeys = rowKeysList.toArray(new String[0]);

            Set<Double> oldestValuesRangeScan = new HashSet<>(), oldestValuesBulkScan = new HashSet<>();
            for (int k = 1; k <= NUM_VERSIONS; k++) {
                Set<Double> latestValuesRangeScan = new HashSet<>();
                NavigableMap<String, NavigableMap<Long, Object>> fieldValues1 = crawlDAO.fetchFieldValues("oKey0", "oKey9", "f1", k);
                for (NavigableMap.Entry<String, NavigableMap<Long, Object>> e : fieldValues1.entrySet()) {
                    latestValuesRangeScan.add((Double) e.getValue().lastEntry().getValue());
                    oldestValuesRangeScan.add((Double) e.getValue().firstEntry().getValue());
                }
                assertEquals(1, latestValuesRangeScan.size(), "When fetching multiple versions of a field, the latest version of field is not as expected");
                Set<Double> latestValuesBulkScan = new HashSet<>();
                Map<String, NavigableMap<Long, Object>> fieldValues2 = crawlDAO.fetchFieldValues(rowKeys, "f1", k);
                for (NavigableMap.Entry<String, NavigableMap<Long, Object>> e : fieldValues2.entrySet()) {
                    latestValuesBulkScan.add((Double) e.getValue().lastEntry().getValue());
                    oldestValuesBulkScan.add((Double) e.getValue().firstEntry().getValue());
                }
                assertEquals(1, latestValuesBulkScan.size(), "When fetching multiple versions of a field, the latest version of field is not as expected");
            }
            assertEquals(NUM_VERSIONS, oldestValuesRangeScan.size(), "When fetching multiple versions of a field through bulk scan, the oldest version of field is not as expected");
            assertEquals(NUM_VERSIONS, oldestValuesBulkScan.size(), "When fetching multiple versions of a field through range scan, the oldest version of field is not as expected");
            assertEquals(oldestValuesRangeScan, oldestValuesBulkScan, "Fetch by array and fetch by range differ");

            // Deletion tests:

            // Written as unversioned, deleted as unversioned:
            final String deleteKey1 = "write_unversioned__delete_unversioned";
            crawlNoVersionDAO.persist(new Crawl(deleteKey1).addF1(10.01));
            crawlNoVersionDAO.delete(deleteKey1);
            assertNull(crawlNoVersionDAO.get(deleteKey1), "Row with key '" + deleteKey1 + "' exists, when written through unversioned DAO and deleted through unversioned DAO!");

            // Written as versioned, deleted as versioned:
            final String deleteKey2 = "write_versioned__delete_versioned";
            crawlDAO.persist(new Crawl(deleteKey2).addF1(10.02));
            crawlDAO.delete(deleteKey2);
            assertNull(crawlNoVersionDAO.get(deleteKey2), "Row with key '" + deleteKey2 + "' exists, when written through versioned DAO and deleted through versioned DAO!");

            // Written as unversioned, deleted as versioned:
            final String deleteKey3 = "write_unversioned__delete_versioned";
            crawlNoVersionDAO.persist(new Crawl(deleteKey3).addF1(10.03));
            crawlDAO.delete(deleteKey3);
            assertNull(crawlNoVersionDAO.get(deleteKey3), "Row with key '" + deleteKey3 + "' exists, when written through unversioned DAO and deleted through versioned DAO!");

            // Written as versioned, deleted as unversioned:
            final String deleteKey4 = "write_versioned__delete_unversioned";
            crawlDAO.persist(new Crawl(deleteKey4).addF1(10.04));
            crawlNoVersionDAO.delete(deleteKey4);
            assertNull(crawlNoVersionDAO.get(deleteKey4), "Row with key '" + deleteKey4 + "' exists, when written through versioned DAO and deleted through unversioned DAO!");
        } finally {
            deleteTables(Crawl.class);
        }
    }

    @Test
    public void testNonStringRowkeys() throws IOException {
        try {
            createTables(Employee.class);
            EmployeeDAO employeeDAO = new EmployeeDAO(connection);
            Employee ePre = new Employee(100L, "E1", (short) 3, System.currentTimeMillis());
            Long rowKey = employeeDAO.persist(ePre);
            Employee ePost = employeeDAO.get(rowKey);
            assertEquals(ePre, ePost, "Object got corrupted after persist and get");
        } finally {
            deleteTables(Employee.class);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        connection.close();
        hBaseCluster.end();
    }
}
