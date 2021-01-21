package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.HBAdmin;
import com.flipkart.hbaseobjectmapper.Records;
import com.flipkart.hbaseobjectmapper.WrappedHBColumnTC;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CitizenDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CitizenSummaryDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CounterDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CrawlDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CrawlNoVersionDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.EmployeeDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import com.flipkart.hbaseobjectmapper.testcases.entities.CitizenSummary;
import com.flipkart.hbaseobjectmapper.testcases.entities.Contact;
import com.flipkart.hbaseobjectmapper.testcases.entities.Counter;
import com.flipkart.hbaseobjectmapper.testcases.entities.Crawl;
import com.flipkart.hbaseobjectmapper.testcases.entities.CrawlNoVersion;
import com.flipkart.hbaseobjectmapper.testcases.entities.Dependents;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.InMemoryHBaseCluster;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.RealHBaseCluster;

import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.a;
import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.e;
import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.l;
import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.m;
import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.nm;
import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.s;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// TODO: fix the tests. Currently, the tests are copied from the sync DAO as-is, but is not idiomatic with real reactive client usage.
class TestsReactiveHBDAO extends BaseHBDAOTests {
    private static AsyncConnection connection;

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
            connection = hBaseCluster.startAsync().join();
            hbAdmin = HBAdmin.create(connection);
            hbAdmin.createNamespace("govt");
            hbAdmin.createNamespace("corp");
        } catch (NumberFormatException e) {
            fail("The environmental variable " + InMemoryHBaseCluster.INMEMORY_CLUSTER_START_TIMEOUT + " is specified incorrectly (Must be numeric)");
        } catch (Exception e) {
            e.printStackTrace(System.err);
            fail(String.format("Failed to connect to HBase. Aborted execution of DAO-related test cases." +
                    "Reason:%n%s", e.getMessage()));
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
                final String rowKey = citizenDao.persist(record).join();
                allRowKeys[i] = rowKey;
                Citizen serDeserRecord = citizenDao.get(rowKey, Integer.MAX_VALUE).join();
                assertEquals(record, serDeserRecord, "Entry got corrupted upon persisting and fetching back");
                for (int numVersions = 1; numVersions <= 4; numVersions++) {
                    final Citizen citizenNVersionsActual = citizenDao.get(rowKey, numVersions).join(), citizenNVersionsExpected = pruneVersionsBeyond(record, numVersions);
                    assertEquals(citizenNVersionsExpected, citizenNVersionsActual, String.format("Mismatch in data between 'record pruned for %d versions' and 'record fetched from HBase for %d versions' for record: %s", numVersions, numVersions, record));
                }
                for (String f : citizenDao.getFields()) { // for each field of the given test object,
                    try {
                        Field field = Citizen.class.getDeclaredField(f);
                        WrappedHBColumnTC hbColumn = new WrappedHBColumnTC(field);
                        field.setAccessible(true);
                        if (hbColumn.isMultiVersioned()) {
                            NavigableMap expected = (NavigableMap) field.get(record);
                            final NavigableMap actual = citizenDao.fetchFieldValue(rowKey, f, Integer.MAX_VALUE).join();
                            assertEquals(expected, actual, String.format("Data for (multi-versioned) field \"%s\" got corrupted upon persisting and fetching back object: %s", field.getName(), record));
                            if (actual == null)
                                continue;
                            if (expectedFieldValues.containsKey(f)) {
                                expectedFieldValues.get(f).put(rowKey, actual);
                            } else {
                                expectedFieldValues.put(f, m(e(rowKey, (Object) actual)));
                            }
                        } else {
                            final Object actual = citizenDao.fetchFieldValue(rowKey, f).join();
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
                    }
                }
            }
            // Test on range scan
            final String startRowKey = allRowKeys[0], endRowKey = allRowKeys[allRowKeys.length - 1];
            List<Citizen> citizens = citizenDao.get(startRowKey, endRowKey, Integer.MAX_VALUE).join();
            for (int i = 0; i < citizens.size(); i++) {
                assertEquals(records.get(i), citizens.get(i), String.format("[range scan] The result of get(%s, %s) returned unexpected entry at position " + i, startRowKey, endRowKey));
            }
            try (Records<Citizen> citizenIterable = citizenDao.records(startRowKey, endRowKey, Integer.MAX_VALUE)) {
                Citizen[] expectedCitizens = citizens.toArray(new Citizen[0]); // this contains all records except the last one
                Citizen[] actualCitizens = Iterables.toArray(citizenIterable, Citizen.class);
                assertArrayEquals(expectedCitizens, actualCitizens, "Fetch directly vs fetch via iterable differ in results [start row key, end row key]");
            }
            Citizen[] allCitizens = citizenDao.get(allRowKeys).map(CompletableFuture::join).toArray(Citizen[]::new);
            List<Citizen> citizensByPrefix = citizenDao.getByPrefix(citizenDao.toBytes("IND#")).join();
            assertArrayEquals(citizensByPrefix.toArray(new Citizen[0]), allCitizens, "get by prefix is returning incorrect result");
            try (Records<Citizen> citizenIterable = citizenDao.recordsByPrefix(citizenDao.toBytes("IND#"))) {
                Citizen[] expectedCitizens = citizenDao.get(allRowKeys).map(CompletableFuture::join).toArray(Citizen[]::new);
                assertArrayEquals(expectedCitizens, Iterables.toArray(citizenIterable, Citizen.class), "Fetch directly vs fetch via iterable differ in results [row key prefix]");
                assertArrayEquals(expectedCitizens, allCitizens, "Results of Get by array of row keys did not match that of list");
            }
            try (Records<Citizen> citizenIterable = citizenDao.records("IND#101", true, "IND#102", true, 1, 1000)) {
                Iterator<Citizen> iterator = citizenIterable.iterator();
                Citizen citizen1 = iterator.next();
                Citizen citizen2 = iterator.next();
                assertEquals(citizenDao.get("IND#101").join(), citizen1, "Get by iterable didn't match get by individual record");
                assertEquals(citizenDao.get("IND#102").join(), citizen2, "Get by iterable didn't match get by individual record");
            }

            assertTrue(Iterables.elementsEqual(
                    citizenDao.records("IND#102", "IND#104"),
                    citizenDao.records("IND#102", true, "IND#104", false, 1, 10)
            ), "Mismatch in result between records() method with and without default options");
            assertTrue(Iterables.elementsEqual(
                    citizenDao.records("IND#102", "IND#104"),
                    citizenDao.get("IND#102", "IND#104").join()
            ), "Mismatch in result between records() and get() methods");

            // Check exists:
            assertTrue(citizenDao.exists("IND#101").join(), "Row key exists, but couldn't be detected");
            assertFalse(citizenDao.exists("IND#100").join(), "Row key doesn't exist");
            assertArrayEquals(new Boolean[]{false, true, true, false, false}, citizenDao.exists(a("IND#100", "IND#101", "IND#102", "IND#121", "IND#141"))
                    .map(CompletableFuture::join).toArray(Boolean[]::new));

            // Range Get vs Bulk Get (Single-version)
            for (String f : citizenDao.getFields()) {
                Map<String, Object> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f).join(),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f).join();
                assertEquals(fieldValuesBulkGetFull, fieldValuesRangeGetFull, "[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for full range");
                Map<String, Object> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#104", "IND#105", "IND#106"), f).join(),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#104", "IND#107", f).join();
                assertEquals(fieldValuesBulkGetPartial, fieldValuesRangeGetPartial, "[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for partial range");
            }

            // Range Get vs Bulk Get (Multi-version)
            for (String f : citizenDao.getFields()) {
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f, Integer.MAX_VALUE).join(),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f, Integer.MAX_VALUE).join();
                assertEquals(fieldValuesBulkGetFull, fieldValuesRangeGetFull, "[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for full range");
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#101", "IND#102", "IND#103"), f, Integer.MAX_VALUE).join(),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#101", "IND#104", f, Integer.MAX_VALUE).join();
                assertEquals(fieldValuesBulkGetPartial, fieldValuesRangeGetPartial, "[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for partial range");
            }

            // Test for a single field (redundant test, but that's ok):
            Map<String, Object> actualSalaries = citizenDao.fetchFieldValues(allRowKeys, "sal").join();
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
            citizenDao.delete(citizenToBeDeleted).join();
            assertNull(citizenDao.get(citizenToBeDeleted.composeRowKey()).join(), "Record was not deleted: " + citizenToBeDeleted);
            List<Citizen> citizensToBeDeleted = Arrays.asList(records.get(1), records.get(2));
            citizenDao.delete(citizensToBeDeleted).map(CompletableFuture::join).forEach(System.out::println);
            assertNull(citizenDao.get(citizensToBeDeleted.get(0).composeRowKey()).join(), "Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(0));
            assertNull(citizenDao.get(citizensToBeDeleted.get(1).composeRowKey()).join(), "Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(1));
            final String rowKey3 = records.get(3).composeRowKey(), rowKey4 = records.get(4).composeRowKey();
            citizenDao.delete(new String[]{rowKey3, rowKey4}).map(CompletableFuture::join).forEach(System.out::println);
            assertNull(citizenDao.get(rowKey3).join(), "Record was not deleted when deleted by 'array of row keys': " + rowKey3);
            assertNull(citizenDao.get(rowKey4).join(), "Record was not deleted when deleted by 'array of row keys': " + rowKey4);
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
            String rowKey = citizenDao.persist(citizenBeforeAppend).join();
            Integer expectedSalary = 30000;
            citizenDao.append(rowKey, "sal", expectedSalary).join();
            try {
                citizenDao.append(rowKey, "blahblah", 5).join();
                fail("An attempt was made to append value a non-existent field. This should have thrown an exception - It didn't.");
            } catch (Exception e) {
                System.out.printf("[egde case] Got error as expected, for non-existent column: %s%n", e.getMessage());
            }
            Citizen citizenAfter1Append = citizenDao.get(rowKey).join();
            assertEquals(expectedSalary, citizenAfter1Append.getSal(), "Append operation didn't work as expected on field 'sal'");
            List<Contact> expectedContacts = l(new Contact("contact1", 23411));
            citizenDao.append(rowKey, "emergencyContacts1", expectedContacts).join();
            Citizen citizenAfter2Append = citizenDao.get(rowKey).join();
            assertEquals(expectedContacts, citizenAfter2Append.getEmergencyContacts1(), "Append operation didn't work as expected on field 'emergencyContacts1'");
            try {
                citizenDao.append(rowKey, m(e("f3", 123L), e("f4", "blah blah blah"))).join();
                fail("An attempt was made to append a BigDecimal field with a String value - This should have thrown an exception - It didn't.");
            } catch (Exception e) {
                System.out.printf("[edge case] Got error as expected, for type mismatch in columns: %s%n", e.getMessage());
            }
            Citizen citizenAfter3Append = citizenDao.get(rowKey).join();
            assertNull(citizenAfter3Append.getF3(), "Append operation broke 'all or none' semantics");
            citizenDao.append(rowKey, m(e("f3", 123L))).join();
            Citizen citizenAfter4Append = citizenDao.get(rowKey).join();
            assertEquals(123L, (long) citizenAfter4Append.getF3(), "Append operation failed for f3");
            citizenDao.append(rowKey, "name", " Kalam").join();
            assertEquals("Abdul Kalam", citizenDao.fetchFieldValue(rowKey, "name").join(), "Append operation failed for name");
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
            final String rowKey = counterDAO.persist(counter).join();
            // Test custom timestamp values:
            assertEquals(counterDAO.get(rowKey, 7).join(), counterDAO.getOnGet(counterDAO.getGet(rowKey).readVersions(7)).join(), "Unexpected values on get (number of versions)");
            assertEquals(nm(e(10L, 10L)), counterDAO.getOnGet(counterDAO.getGet(rowKey).setTimestamp(10)).join().getValue(), "Unexpected values on get (given timestamp)");
            assertEquals(Arrays.asList(new Counter("c1", nm(e(1L, 1L), e(2L, 2L), e(3L, 3L), e(4L, 4L))), new Counter("c1", nm(e(3L, 3L), e(4L, 4L)))),
                    counterDAO.getOnGets(Arrays.asList(counterDAO.getGet(rowKey).setTimeRange(1, 5).readAllVersions(), counterDAO.getGet(rowKey).setTimeRange(1, 5).readVersions(2))).map(CompletableFuture::join).collect(Collectors.toList()),
                    "Unexpected values on bulk get");
            // Test increment features:
            assertEquals(1L, counterDAO.increment(rowKey, "var", 1L).join(), "Increment didn't apply - basic");
            assertEquals(1L, (long) counterDAO.fetchFieldValue(rowKey, "var").join(), "Increment apply didn't reflect in fetch field - basic");
            assertEquals(3L, counterDAO.increment(rowKey, "var", 2L, Durability.SKIP_WAL).join(), "Increment didn't apply - with durability flag");
            assertEquals(3L, counterDAO.fetchFieldValue(rowKey, "var").join(), "Increment apply didn't reflect in fetch field - with durability flag");
            Increment increment = counterDAO.getIncrement(rowKey).addColumn(Bytes.toBytes("a"), Bytes.toBytes("var"), 5L);
            Counter persistedCounter = counterDAO.increment(increment).join();
            assertEquals(8L, persistedCounter.getVar(), "Increment didn't reflect in object get - native way");
            assertEquals(8L, (long) counterDAO.fetchFieldValue(rowKey, "var").join(), "Increment didn't apply in fetch field - native way");
            try {
                counterDAO.increment(rowKey, "badvarI", 4L).join();
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
            crawlNoVersionDAO.persist(objs).map(CompletableFuture::join).forEach(System.out::print);
            Crawl crawl = crawlDAO.get("key", NUM_VERSIONS).join();
            assertEquals(1.0, crawl.getF1Versioned().values().iterator().next(), 1e-9, "Issue with version history implementation when written as unversioned and read as versioned");
            crawlDAO.delete("key").join();
            Crawl versioned = crawlDAO.get("key").join();
            assertNull(versioned, "Deleted row (with key " + versioned + ") still exists when accessed as versioned DAO");
            CrawlNoVersion versionless = crawlNoVersionDAO.get("key").join();
            assertNull(versionless, "Deleted row (with key " + versionless + ") still exists when accessed as versionless DAO");
            // Written as versioned, read as unversioned+versioned
            Crawl crawl2 = new Crawl("key2");
            long timestamp = System.currentTimeMillis();
            long i = 0;
            for (Double n : testNumbers) {
                crawl2.addF1(timestamp + i, n);
                i++;
            }
            crawlDAO.persist(crawl2).join();
            CrawlNoVersion crawlNoVersion = crawlNoVersionDAO.get("key2").join();
            assertEquals(crawlNoVersion.getF1(), testNumbers[testNumbers.length - 1], "Entry with the highest version (i.e. timestamp) isn't the one that was returned by DAO get");
            assertArrayEquals(testNumbersOfRange, crawlDAO.get("key2", NUM_VERSIONS).join().getF1Versioned().values().toArray(), "Issue with version history implementation when written as versioned and read as unversioned");

            List<String> rowKeysList = new ArrayList<>();
            for (int v = 0; v <= 9; v++) {
                for (int k = 1; k <= 4; k++) {
                    String key = "oKey" + k;
                    crawlDAO.persist(new Crawl(key).addF1((double) v)).join();
                    rowKeysList.add(key);
                }
            }
            String[] rowKeys = rowKeysList.toArray(new String[0]);

            Set<Double> oldestValuesRangeScan = new HashSet<>(), oldestValuesBulkScan = new HashSet<>();
            for (int k = 1; k <= NUM_VERSIONS; k++) {
                Set<Double> latestValuesRangeScan = new HashSet<>();
                NavigableMap<String, NavigableMap<Long, Object>> fieldValues1 = crawlDAO.fetchFieldValues("oKey0", "oKey9", "f1", k).join();
                for (NavigableMap.Entry<String, NavigableMap<Long, Object>> e : fieldValues1.entrySet()) {
                    latestValuesRangeScan.add((Double) e.getValue().lastEntry().getValue());
                    oldestValuesRangeScan.add((Double) e.getValue().firstEntry().getValue());
                }
                assertEquals(1, latestValuesRangeScan.size(), "When fetching multiple versions of a field, the latest version of field is not as expected");
                Set<Double> latestValuesBulkScan = new HashSet<>();
                Map<String, NavigableMap<Long, Object>> fieldValues2 = crawlDAO.fetchFieldValues(rowKeys, "f1", k).join();
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
            crawlNoVersionDAO.persist(new Crawl(deleteKey1).addF1(10.01)).join();
            crawlNoVersionDAO.delete(deleteKey1).join();
            assertNull(crawlNoVersionDAO.get(deleteKey1).join(), "Row with key '" + deleteKey1 + "' exists, when written through unversioned DAO and deleted through unversioned DAO!");

            // Written as versioned, deleted as versioned:
            final String deleteKey2 = "write_versioned__delete_versioned";
            crawlDAO.persist(new Crawl(deleteKey2).addF1(10.02)).join();
            crawlDAO.delete(deleteKey2).join();
            assertNull(crawlNoVersionDAO.get(deleteKey2).join(), "Row with key '" + deleteKey2 + "' exists, when written through versioned DAO and deleted through versioned DAO!");

            // Written as unversioned, deleted as versioned:
            final String deleteKey3 = "write_unversioned__delete_versioned";
            crawlNoVersionDAO.persist(new Crawl(deleteKey3).addF1(10.03)).join();
            crawlDAO.delete(deleteKey3).join();
            assertNull(crawlNoVersionDAO.get(deleteKey3).join(), "Row with key '" + deleteKey3 + "' exists, when written through unversioned DAO and deleted through versioned DAO!");

            // Written as versioned, deleted as unversioned:
            final String deleteKey4 = "write_versioned__delete_unversioned";
            crawlDAO.persist(new Crawl(deleteKey4).addF1(10.04)).join();
            crawlNoVersionDAO.delete(deleteKey4).join();
            assertNull(crawlNoVersionDAO.get(deleteKey4).join(), "Row with key '" + deleteKey4 + "' exists, when written through versioned DAO and deleted through unversioned DAO!");
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
            Long rowKey = employeeDAO.persist(ePre).join();
            Employee ePost = employeeDAO.get(rowKey).join();
            assertEquals(ePre, ePost, "Object got corrupted after persist and get");
        } finally {
            deleteTables(Employee.class);
        }
    }
}
