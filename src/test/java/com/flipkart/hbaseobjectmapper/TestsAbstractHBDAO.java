package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.daos.CitizenSummaryDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestsAbstractHBDAO {
    HBaseTestingUtility utility = new HBaseTestingUtility();
    CitizenDAO citizenDao;
    CitizenSummaryDAO citizenSummaryDAO;
    private List<Citizen> testObjs = TestObjects.validObjs;

    @Before
    public void setup() throws Exception {
        utility.startMiniCluster();
        utility.createTable("citizens".getBytes(), new byte[][]{"main".getBytes(), "optional".getBytes()});
        utility.createTable("citizen_summary".getBytes(), new byte[][]{"a".getBytes()});
        Configuration configuration = utility.getConfiguration();
        citizenDao = new CitizenDAO(configuration);
        citizenSummaryDAO = new CitizenSummaryDAO(configuration);
    }

    @Test
    public void testTableParticulars() {
        assertEquals(citizenDao.getTableName(), "citizens");
        assertEquals(citizenSummaryDAO.getTableName(), "citizen_summary");
        assertTrue(TestUtil.setEquals(citizenDao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
        assertTrue(TestUtil.setEquals(citizenSummaryDAO.getColumnFamilies(), Sets.newHashSet("a")));
    }

    @Test
    public void testHBaseDAO() throws Exception {
        String[] rowKeys = new String[testObjs.size()];
        Map<String, Object> expectedNames = new HashMap<String, Object>();
        for (int i = 0; i < testObjs.size(); i++) {
            Citizen e = testObjs.get(i);
            String rowKey = citizenDao.persist(e);
            rowKeys[i] = rowKey;
            expectedNames.put(rowKey, e.getName());
            Citizen pe = citizenDao.get(rowKey);
            assertEquals("Entry got corrupted upon persisting and fetching back", pe, e);
            for (String f : citizenDao.getFields()) {
                Field field = Citizen.class.getDeclaredField(f);
                field.setAccessible(true);
                assertEquals("Field data corrupted upon persisting and fetching back", field.get(e), citizenDao.fetchFieldValue(rowKey, f));
            }
        }
        Map<String, Object> actualNames = citizenDao.fetchFieldValues(rowKeys, "name");
        assertTrue("Invalid data returned when column values were fetched in bulk", TestUtil.mapEquals(actualNames, expectedNames));
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

    @After
    public void tearDown() throws Exception {
        utility.shutdownMiniCluster();
    }
}
