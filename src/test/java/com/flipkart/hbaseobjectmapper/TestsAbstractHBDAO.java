package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.daos.CitizenSummaryDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TestsAbstractHBDAO {
    HBaseTestingUtility utility = new HBaseTestingUtility();
    CitizenDAO citizenDao;
    CitizenSummaryDAO citizenSummaryDAO;
    List<Citizen> testObjs = TestObjects.validObjs;
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

    @Before
    public void setup() throws Exception {
        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(new ClusterStarter(utility)).get(CLUSTER_START_TIMEOUT, TimeUnit.SECONDS);
            utility.createTable("citizens".getBytes(), new byte[][]{"main".getBytes(), "optional".getBytes()});
            utility.createTable("citizen_summary".getBytes(), new byte[][]{"a".getBytes()});
            Configuration configuration = utility.getConfiguration();
            citizenDao = new CitizenDAO(configuration);
            citizenSummaryDAO = new CitizenSummaryDAO(configuration);
        } catch (TimeoutException tox) {
            fail("In-memory HBase Test Cluster could not be started in " + CLUSTER_START_TIMEOUT + " seconds - aborted execution of DAO-related test cases");
        }
    }

    public void testTableParticulars() {
        assertEquals(citizenDao.getTableName(), "citizens");
        assertEquals(citizenSummaryDAO.getTableName(), "citizen_summary");
        assertTrue(TestUtil.setEquals(citizenDao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
        assertTrue(TestUtil.setEquals(citizenSummaryDAO.getColumnFamilies(), Sets.newHashSet("a")));
    }

    public void testHBaseDAO() throws Exception {
        String[] rowKeys = new String[testObjs.size()];
        Map<String, Map<String, Object>> expectedFieldValues = new HashMap<String, Map<String, Object>>();
        for (int i = 0; i < testObjs.size(); i++) {
            Citizen e = testObjs.get(i);
            final String rowKey = citizenDao.persist(e);
            rowKeys[i] = rowKey;
            Citizen pe = citizenDao.get(rowKey);
            assertEquals("Entry got corrupted upon persisting and fetching back", pe, e);
            for (String f : citizenDao.getFields()) {
                Field field = Citizen.class.getDeclaredField(f);
                field.setAccessible(true);
                final Object actual = citizenDao.fetchFieldValue(rowKey, f);
                assertEquals("Field data corrupted upon persisting and fetching back", field.get(e), actual);
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
        assertArrayEquals("Data mismatch between single and bulk 'get' calls", testObjs.toArray(), (Object[]) citizenDao.get(rowKeys));
        Citizen citizenToBeDeleted = testObjs.get(0);
        citizenDao.delete(citizenToBeDeleted);
        assertNull("Record was not deleted: " + citizenToBeDeleted, citizenDao.get(citizenToBeDeleted.composeRowKey()));
        Citizen[] citizensToBeDeleted = new Citizen[]{testObjs.get(1), testObjs.get(2)};
        citizenDao.delete(citizensToBeDeleted);
        assertNull("Record was not deleted: " + citizensToBeDeleted[0], citizenDao.get(citizensToBeDeleted[0].composeRowKey()));
        assertNull("Record was not deleted: " + citizensToBeDeleted[1], citizenDao.get(citizensToBeDeleted[1].composeRowKey()));
    }

    @Test
    public void test() throws Exception {
        testTableParticulars();
        testHBaseDAO();
    }

    @After
    public void tearDown() throws Exception {
        utility.shutdownMiniCluster();
    }
}
