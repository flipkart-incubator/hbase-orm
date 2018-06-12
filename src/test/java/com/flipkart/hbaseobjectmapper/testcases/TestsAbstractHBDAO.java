package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.WrappedHBColumnTC;
import com.flipkart.hbaseobjectmapper.codec.JavaObjectStreamCodec;
import com.flipkart.hbaseobjectmapper.testcases.daos.*;
import com.flipkart.hbaseobjectmapper.testcases.entities.*;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.HBaseCluster;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.InMemoryHBaseCluster;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.RealHBaseCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;

import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.*;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class TestsAbstractHBDAO {
    private static Configuration configuration;
    private static HBaseCluster hBaseCluster;

    @BeforeClass
    public static void setup() {
        try {
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
            configuration = hBaseCluster.init();
        } catch (NumberFormatException e) {
            fail("The environmental variable " + InMemoryHBaseCluster.INMEMORY_CLUSTER_START_TIMEOUT + " is specified incorrectly (Must be numeric)");
        } catch (Exception e) {
            fail("Failed to connect to HBase. Aborted execution of DAO-related test cases. Reason:\n" + e.getMessage());
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
        hBaseCluster.createTable("citizens", m(e("main", 1), e("optional", 3)));
        hBaseCluster.createTable("citizens_summary", m(e("a", 3)));
        try (
                CitizenDAO citizenDao = new CitizenDAO(configuration);
                CitizenSummaryDAO citizenSummaryDAO = new CitizenSummaryDAO(configuration)
        ) {
            assertEquals("citizens", citizenDao.getTableName());
            final Set<String> columnFamiliesCitizen = citizenDao.getColumnFamiliesAndVersions().keySet(), columnFamiliesCitizenSummary = citizenSummaryDAO.getColumnFamiliesAndVersions().keySet();
            assertEquals("Issue with column families of 'citizens' table\n" + columnFamiliesCitizen, s("main", "optional"), columnFamiliesCitizen);
            assertEquals("citizens_summary", citizenSummaryDAO.getTableName());
            assertEquals("Issue with column families of 'citizens_summary' table\n" + columnFamiliesCitizenSummary, s("a"), columnFamiliesCitizenSummary);
            final List<Citizen> records = TestObjects.validCitizenObjects;
            String[] allRowKeys = new String[records.size()];
            Map<String, Map<String, Object>> expectedFieldValues = new HashMap<>();
            for (int i = 0; i < records.size(); i++) { // for each test object,
                Citizen record = records.get(i);
                final String rowKey = citizenDao.persist(record);
                allRowKeys[i] = rowKey;
                Citizen serDeserRecord = citizenDao.get(rowKey, Integer.MAX_VALUE);
                assertEquals("Entry got corrupted upon persisting and fetching back", record, serDeserRecord);
                for (int numVersions = 1; numVersions <= 4; numVersions++) {
                    final Citizen citizenNVersionsActual = citizenDao.get(rowKey, numVersions), citizenNVersionsExpected = pruneVersionsBeyond(record, numVersions);
                    assertEquals("Mismatch in data between 'record pruned for " + numVersions + " versions' and 'record fetched from HBase for " + numVersions + "versions' for record: " + record, citizenNVersionsExpected, citizenNVersionsActual);
                }
                for (String f : citizenDao.getFields()) { // for each field of the given test object,
                    try {
                        Field field = Citizen.class.getDeclaredField(f);
                        WrappedHBColumnTC hbColumn = new WrappedHBColumnTC(field);
                        field.setAccessible(true);
                        if (hbColumn.isMultiVersioned()) {
                            NavigableMap expected = (NavigableMap) field.get(record);
                            final NavigableMap actual = citizenDao.fetchFieldValue(rowKey, f, Integer.MAX_VALUE);
                            assertEquals(String.format("Data for (multi-versioned) field \"%s\" got corrupted upon persisting and fetching back object: %s", field.getName(), record), expected, actual);
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
                            assertEquals(String.format("Data for field \"%s\" got corrupted upon persisting and fetching back object: %s", field.getName(), record), expected, actual);
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
                assertEquals(String.format("[range scan] The result of get(%s, %s) returned unexpected entry at position " + i, startRowKey, endRowKey), records.get(i), citizens.get(i));
            }

            // Range Get vs Bulk Get (Single-version)
            for (String f : citizenDao.getFields()) {
                Map<String, Object> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f);
                assertEquals("[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for full range", fieldValuesBulkGetFull, fieldValuesRangeGetFull);
                Map<String, Object> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#104", "IND#105", "IND#106"), f),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#104", "IND#107", f);
                assertEquals("[Field " + f + "] Difference between 'fetch by array of row keys' and 'fetch by range of row keys' when fetched for partial range", fieldValuesBulkGetPartial, fieldValuesRangeGetPartial);
            }

            // Range Get vs Bulk Get (Multi-version)
            for (String f : citizenDao.getFields()) {
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetFull = citizenDao.fetchFieldValues(allRowKeys, f, Integer.MAX_VALUE),
                        fieldValuesRangeGetFull = citizenDao.fetchFieldValues("A", "z", f, Integer.MAX_VALUE);
                assertEquals("[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for full range", fieldValuesBulkGetFull, fieldValuesRangeGetFull);
                Map<String, NavigableMap<Long, Object>> fieldValuesBulkGetPartial = citizenDao.fetchFieldValues(a("IND#101", "IND#102", "IND#103"), f, Integer.MAX_VALUE),
                        fieldValuesRangeGetPartial = citizenDao.fetchFieldValues("IND#101", "IND#104", f, Integer.MAX_VALUE);
                assertEquals("[Field " + f + "] Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys' when fetched for partial range", fieldValuesBulkGetPartial, fieldValuesRangeGetPartial);
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
            assertNull("Record was not deleted: " + citizenToBeDeleted, citizenDao.get(citizenToBeDeleted.composeRowKey()));
            List<Citizen> citizensToBeDeleted = Arrays.asList(records.get(1), records.get(2));
            citizenDao.delete(citizensToBeDeleted);
            assertNull("Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(0), citizenDao.get(citizensToBeDeleted.get(0).composeRowKey()));
            assertNull("Record was not deleted when deleted by 'list of objects': " + citizensToBeDeleted.get(1), citizenDao.get(citizensToBeDeleted.get(1).composeRowKey()));
            final String rowKey3 = records.get(3).composeRowKey(), rowKey4 = records.get(4).composeRowKey();
            citizenDao.delete(new String[]{rowKey3, rowKey4});
            assertNull("Record was not deleted when deleted by 'array of row keys': " + rowKey3, citizenDao.get(rowKey3));
            assertNull("Record was not deleted when deleted by 'array of row keys': " + rowKey4, citizenDao.get(rowKey4));
        }
    }

    @Test
    public void testCustom() throws IOException {
        hBaseCluster.createTable("counters", m(e("a", 10)));
        try (
                CounterDAO counterDAO = new CounterDAO(configuration)
        ) {
            Counter counter = new Counter("c1");
            for (int i = 1; i <= 10; i++) {
                counter.set((long) i, (long) i);
            }
            final String rowKey = counterDAO.persist(counter);
            assertEquals("Unexpected values on get (number of versions)", counterDAO.get(rowKey, 7), counterDAO.getOnGet(counterDAO.getGet(rowKey).setMaxVersions(7)));
            assertEquals("Unexpected values on get (given timestamp)", nm(e(10L, 10L)), counterDAO.getOnGet(counterDAO.getGet(rowKey).setTimeStamp(10)).getValue());
            assertEquals("Unexpected values on bulk get", Arrays.asList(new Counter("c1", nm(e(1L, 1L), e(2L, 2L), e(3L, 3L), e(4L, 4L))), new Counter("c1", nm(e(3L, 3L), e(4L, 4L)))),
                    counterDAO.getOnGets(Arrays.asList(counterDAO.getGet(rowKey).setTimeRange(1, 5).setMaxVersions(), counterDAO.getGet(rowKey).setTimeRange(1, 5).setMaxVersions(2))));
        }
    }

    @Test
    public void testVersioning() throws IOException {
        hBaseCluster.createTable("crawls", m(e("a", 3)));
        try (
                CrawlDAO crawlDAO = new CrawlDAO(configuration);
                CrawlNoVersionDAO crawlNoVersionDAO = new CrawlNoVersionDAO(configuration)
        ) {
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
            assertEquals("Issue with version history implementation when written as unversioned and read as versioned", 1.0, crawl.getF1().values().iterator().next(), 1e-9);
            crawlDAO.delete("key");
            Crawl versioned = crawlDAO.get("key");
            assertNull("Deleted row (with key " + versioned + ") still exists when accessed as versioned DAO", versioned);
            CrawlNoVersion versionless = crawlNoVersionDAO.get("key");
            assertNull("Deleted row (with key " + versionless + ") still exists when accessed as versionless DAO", versionless);
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
            assertEquals("Entry with the highest version (i.e. timestamp) isn't the one that was returned by DAO get", crawlNoVersion.getF1(), testNumbers[testNumbers.length - 1]);
            assertArrayEquals("Issue with version history implementation when written as versioned and read as unversioned", testNumbersOfRange, crawlDAO.get("key2", NUM_VERSIONS).getF1().values().toArray());

            List<String> rowKeysList = new ArrayList<>();
            for (int v = 0; v <= 9; v++) {
                for (int k = 1; k <= 4; k++) {
                    String key = "oKey" + k;
                    crawlDAO.persist(new Crawl(key).addF1((double) v));
                    rowKeysList.add(key);
                }
            }
            String[] rowKeys = rowKeysList.toArray(new String[rowKeysList.size()]);

            Set<Double> oldestValuesRangeScan = new HashSet<>(), oldestValuesBulkScan = new HashSet<>();
            for (int k = 1; k <= NUM_VERSIONS; k++) {
                Set<Double> latestValuesRangeScan = new HashSet<>();
                NavigableMap<String, NavigableMap<Long, Object>> fieldValues1 = crawlDAO.fetchFieldValues("oKey0", "oKey9", "f1", k);
                for (NavigableMap.Entry<String, NavigableMap<Long, Object>> e : fieldValues1.entrySet()) {
                    latestValuesRangeScan.add((Double) e.getValue().lastEntry().getValue());
                    oldestValuesRangeScan.add((Double) e.getValue().firstEntry().getValue());
                }
                assertEquals("When fetching multiple versions of a field, the latest version of field is not as expected", 1, latestValuesRangeScan.size());
                Set<Double> latestValuesBulkScan = new HashSet<>();
                Map<String, NavigableMap<Long, Object>> fieldValues2 = crawlDAO.fetchFieldValues(rowKeys, "f1", k);
                for (NavigableMap.Entry<String, NavigableMap<Long, Object>> e : fieldValues2.entrySet()) {
                    latestValuesBulkScan.add((Double) e.getValue().lastEntry().getValue());
                    oldestValuesBulkScan.add((Double) e.getValue().firstEntry().getValue());
                }
                assertEquals("When fetching multiple versions of a field, the latest version of field is not as expected", 1, latestValuesBulkScan.size());
            }
            assertEquals("When fetching multiple versions of a field through bulk scan, the oldest version of field is not as expected", NUM_VERSIONS, oldestValuesRangeScan.size());
            assertEquals("When fetching multiple versions of a field through range scan, the oldest version of field is not as expected", NUM_VERSIONS, oldestValuesBulkScan.size());
            assertEquals("Fetch by array and fetch by range differ", oldestValuesRangeScan, oldestValuesBulkScan);

            // Deletion tests:

            // Written as unversioned, deleted as unversioned:
            final String deleteKey1 = "write_unversioned__delete_unversioned";
            crawlNoVersionDAO.persist(new Crawl(deleteKey1).addF1(10.01));
            crawlNoVersionDAO.delete(deleteKey1);
            assertNull("Row with key '" + deleteKey1 + "' exists, when written through unversioned DAO and deleted through unversioned DAO!", crawlNoVersionDAO.get(deleteKey1));

            // Written as versioned, deleted as versioned:
            final String deleteKey2 = "write_versioned__delete_versioned";
            crawlDAO.persist(new Crawl(deleteKey2).addF1(10.02));
            crawlDAO.delete(deleteKey2);
            assertNull("Row with key '" + deleteKey2 + "' exists, when written through versioned DAO and deleted through versioned DAO!", crawlNoVersionDAO.get(deleteKey2));

            // Written as unversioned, deleted as versioned:
            final String deleteKey3 = "write_unversioned__delete_versioned";
            crawlNoVersionDAO.persist(new Crawl(deleteKey3).addF1(10.03));
            crawlDAO.delete(deleteKey3);
            assertNull("Row with key '" + deleteKey3 + "' exists, when written through unversioned DAO and deleted through versioned DAO!", crawlNoVersionDAO.get(deleteKey3));

            // Written as versioned, deleted as unversioned:
            final String deleteKey4 = "write_versioned__delete_unversioned";
            crawlDAO.persist(new Crawl(deleteKey4).addF1(10.04));
            crawlNoVersionDAO.delete(deleteKey4);
            assertNull("Row with key '" + deleteKey4 + "' exists, when written through versioned DAO and deleted through unversioned DAO!", crawlNoVersionDAO.get(deleteKey4));
        }

    }

    @Test
    public void testNonStringRowkeys() throws IOException {
        hBaseCluster.createTable("employees", m(e("a", 1)));
        try (
                EmployeeDAO employeeDAO = new EmployeeDAO(configuration)
        ) {
            Employee ePre = new Employee(100L, "E1", (short) 3, System.currentTimeMillis());
            Long rowKey = employeeDAO.persist(ePre);
            Employee ePost = employeeDAO.get(rowKey);
            assertEquals("Object got corrupted ", ePre, ePost);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hBaseCluster.end();
    }
}
