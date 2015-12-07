package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.daos.CitizenSummaryDAO;
import com.flipkart.hbaseobjectmapper.daos.CrawlDAO;
import com.flipkart.hbaseobjectmapper.daos.CrawlNoVersionDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.flipkart.hbaseobjectmapper.entities.Crawl;
import com.flipkart.hbaseobjectmapper.entities.CrawlNoVersion;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TestsAbstractHBDAO {
    HBaseTestingUtility utility = new HBaseTestingUtility();
    Configuration configuration;
    CitizenDAO citizenDao;
    CitizenSummaryDAO citizenSummaryDAO;
    CrawlDAO crawlDAO;
    CrawlNoVersionDAO crawlNoVersionDAO;
    List<Citizen> testObjs = TestObjects.validObjsNoVersion;
    final static long CLUSTER_START_TIMEOUT = 30;

    class ClusterStarter implements Callable<MiniHBaseCluster> {
        private final HBaseTestingUtility utility;

        public ClusterStarter(HBaseTestingUtility utility) {
            this.utility = utility;
        }

        @Override
        public MiniHBaseCluster call() throws Exception {
            System.out.println("Starting HBase Test Cluster (in-memory)...");
            return utility.startMiniCluster();
        }
    }

    private interface TablesCreator {
        void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException;
    }

    private static class ActualTablesCreator implements TablesCreator {
        private HBaseAdmin hBaseAdmin;

        private ActualTablesCreator(HBaseAdmin hBaseAdmin) {
            this.hBaseAdmin = hBaseAdmin;
        }

        @Override
        public void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException {
            if (hBaseAdmin.tableExists(tableName)) {
                System.out.format("Disabling table '%s': ", tableName);
                hBaseAdmin.disableTable(tableName);
                System.out.println("[DONE]");
                System.out.format("Deleting table '%s': ", tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println("[DONE]");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String columnFamily : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily).setMaxVersions(numVersions));
            }
            System.out.format("Creating table '%s': ", tableName);
            hBaseAdmin.createTable(tableDescriptor);
            System.out.println("[DONE]");
        }
    }

    private static class InMemoryTablesCreator implements TablesCreator {

        private HBaseTestingUtility utility;

        private InMemoryTablesCreator(HBaseTestingUtility utility) {
            this.utility = utility;
        }

        @Override
        public void createTable(String tableName, String[] columnFamilies, int numVersions) throws IOException {
            byte[][] columnFamiliesBytes = new byte[columnFamilies.length][];
            for (int i = 0; i < columnFamilies.length; i++) {
                columnFamiliesBytes[i] = columnFamilies[i].getBytes();
            }
            utility.createTable(tableName.getBytes(), columnFamiliesBytes, numVersions);
        }
    }

    @Before
    public void setup() {
        String useRegularHBaseClient = System.getenv("USE_REGULAR_HBASE_CLIENT");
        try {
            TablesCreator tablesCreator = null;
            if (useRegularHBaseClient != null && (useRegularHBaseClient.equals("1") || useRegularHBaseClient.equalsIgnoreCase("true"))) {
                configuration = getRegularHBaseClient();
                System.out.println("Creating HBase admin");
                HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
                System.out.println("Recreating tables on HBase");
                tablesCreator = new ActualTablesCreator(hBaseAdmin);
            } else {
                try {
                    System.out.println("Starting test cluster");
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    executorService.submit(new ClusterStarter(utility)).get(CLUSTER_START_TIMEOUT, TimeUnit.SECONDS);
                    configuration = utility.getConfiguration();
                    System.out.println("Creating tables on HBase test cluster");
                    tablesCreator = new InMemoryTablesCreator(utility);
                } catch (TimeoutException tox) {
                    fail("In-memory HBase Test Cluster could not be started in " + CLUSTER_START_TIMEOUT + " seconds - aborted execution of DAO-related test cases");
                }
            }
            createDAOs(tablesCreator);
        } catch (Exception ioex) {
            ioex.printStackTrace();
            fail("Could not setup HBase for testing");
        }
    }

    private void createDAOs(TablesCreator tablesCreator) throws IOException {
        tablesCreator.createTable("citizens", new String[]{"main", "optional"}, 1);
        citizenDao = new CitizenDAO(configuration);
        tablesCreator.createTable("citizen_summary", new String[]{"a"}, 1);
        citizenSummaryDAO = new CitizenSummaryDAO(configuration);
        tablesCreator.createTable("crawl", new String[]{"a"}, 3);
        crawlDAO = new CrawlDAO(configuration);
        crawlNoVersionDAO = new CrawlNoVersionDAO(configuration);
    }

    public void testTableParticulars() {
        assertEquals(citizenDao.getTableName(), "citizens");
        assertTrue("Issue with column families of 'citizens' table\n" + citizenDao.getColumnFamilies(), TestUtil.setEquals(citizenDao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
        assertEquals(citizenSummaryDAO.getTableName(), "citizen_summary");
        assertTrue("Issue with column families of 'citizen_summary' table\n" + citizenSummaryDAO.getColumnFamilies(), TestUtil.setEquals(citizenSummaryDAO.getColumnFamilies(), Sets.newHashSet("a")));
    }

    public void testHBaseDAO() throws IOException {
        String[] rowKeys = new String[testObjs.size()];
        Map<String, Map<String, Object>> expectedFieldValues = new HashMap<String, Map<String, Object>>();
        for (int i = 0; i < testObjs.size(); i++) {
            Citizen e = testObjs.get(i);
            try {
                final String rowKey = citizenDao.persist(e);
                rowKeys[i] = rowKey;
                Citizen pe = citizenDao.get(rowKey);
                assertEquals("Entry got corrupted upon persisting and fetching back", e, pe);
                for (String f : citizenDao.getFields()) {
                    try {
                        Field field = Citizen.class.getDeclaredField(f);
                        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
                        field.setAccessible(true);
                        final Object actual = citizenDao.fetchFieldValue(rowKey, f);
                        Object expected = field.get(e);
                        if (hbColumn.isMultiVersioned()) {
                            NavigableMap columnHistory = ((NavigableMap) field.get(e));
                            if (columnHistory != null && columnHistory.size() > 0) {
                                expected = columnHistory.lastEntry().getValue();
                            }
                        }
                        assertEquals("Field data corrupted upon persisting and fetching back", expected, actual);
                        if (actual == null) continue;
                        if (!expectedFieldValues.containsKey(f)) {
                            expectedFieldValues.put(f, new HashMap<String, Object>() {
                                {
                                    put(rowKey, actual);
                                }
                            });
                        } else {
                            expectedFieldValues.get(f).put(rowKey, actual);
                        }
                    } catch (IllegalAccessException e1) {
                        e1.printStackTrace();
                        fail("Can't get field " + f + " from object " + e);
                    } catch (NoSuchFieldException e1) {
                        e1.printStackTrace();
                        fail("Field missing: " + f);
                    } catch (IOException ioex) {
                        ioex.printStackTrace();
                        fail("Could not fetch field '" + f + "' for row '" + rowKey + "'");
                    }
                }
            } catch (IOException ioex) {
                fail();
            }
        }
        List<Citizen> citizens = citizenDao.get(rowKeys[0], rowKeys[rowKeys.length - 1]);
        for (int i = 0; i < citizens.size(); i++) {
            assertEquals("When retrieved in bulk (range scan), we have unexpected entry", citizens.get(i), testObjs.get(i));
        }
        for (String f : citizenDao.getFields()) {
            Map<String, Object> actualFieldValues = citizenDao.fetchFieldValues(rowKeys, f);
            Map<String, Object> actualFieldValuesScanned = citizenDao.fetchFieldValues("A", "z", f);
            assertTrue(String.format("Invalid data returned when values for column \"%s\" were fetched in bulk\nExpected: %s\nActual: %s", f, expectedFieldValues.get(f), actualFieldValues), TestUtil.mapEquals(actualFieldValues, expectedFieldValues.get(f)));
            assertTrue("Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys'", TestUtil.mapEquals(actualFieldValues, actualFieldValuesScanned));
        }
        Map<String, Object> actualSalaries = citizenDao.fetchFieldValues(rowKeys, "sal");
        long actualSumOfSalaries = 0;
        for (Object s : actualSalaries.values()) {
            actualSumOfSalaries += s == null ? 0 : (Integer) s;
        }
        long expectedSumOfSalaries = 0;
        for (Citizen c : testObjs) {
            expectedSumOfSalaries += c.getSal() == null ? 0 : c.getSal();
        }
        assertEquals(expectedSumOfSalaries, actualSumOfSalaries);
        assertArrayEquals("Data mismatch between single and bulk 'get' calls", testObjs.toArray(), citizenDao.get(rowKeys));
        assertEquals("Data mismatch between List and array bulk variants of 'get' calls", testObjs, citizenDao.get(Arrays.asList(rowKeys)));
        Citizen citizenToBeDeleted = testObjs.get(0);
        citizenDao.delete(citizenToBeDeleted);
        assertNull("Record was not deleted: " + citizenToBeDeleted, citizenDao.get(citizenToBeDeleted.composeRowKey()));
        Citizen[] citizensToBeDeleted = new Citizen[]{testObjs.get(1), testObjs.get(2)};
        citizenDao.delete(citizensToBeDeleted);
        assertNull("Record was not deleted: " + citizensToBeDeleted[0], citizenDao.get(citizensToBeDeleted[0].composeRowKey()));
        assertNull("Record was not deleted: " + citizensToBeDeleted[1], citizenDao.get(citizensToBeDeleted[1].composeRowKey()));
    }

    public void testHBaseMultiVersionDAO() throws Exception {
        Double[] testNumbers = new Double[]{-1.0, Double.MAX_VALUE, Double.MIN_VALUE, 3.14159, 2.71828, 1.0};
        Double[] testNumbersOfRange = Arrays.copyOfRange(testNumbers, testNumbers.length - 3, testNumbers.length);
        // Written as unversioned, read as versioned
        List<HBRecord> objs = new ArrayList<HBRecord>();
        for (Double n : testNumbers) {
            objs.add(new CrawlNoVersion("key").setF1(n));
        }
        crawlNoVersionDAO.persist(objs);
        Crawl crawl = crawlDAO.get("key", 3);
        Double[] outputNumbers = crawl.getF1().values().toArray(new Double[3]);
        assertArrayEquals("Issue with version history implementation when written as unversioned and read as versioned", testNumbersOfRange, outputNumbers);
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
        assertArrayEquals("Issue with version history implementation when written as versioned and read as unversioned", testNumbersOfRange, crawlDAO.get("key2", 3).getF1().values().toArray());
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


    @Test
    public void test() throws Exception {
        System.out.println("Testing table attributes");
        testTableParticulars();
        System.out.println("Testing data access objects");
        testHBaseDAO();
        System.out.println("Testing multi-versioned data access objects");
        testHBaseMultiVersionDAO();
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("Shutting down in-memory cluster");
        utility.shutdownMiniCluster();
    }

    private Configuration getRegularHBaseClient() {
        return HBaseConfiguration.create();
    }
}
