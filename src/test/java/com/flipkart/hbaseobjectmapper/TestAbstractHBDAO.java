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

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAbstractHBDAO {
    public static <T> boolean setEquals(Set<T> leftSet, Set<T> rightSet) {
        return !(leftSet == null || rightSet == null || leftSet.size() != rightSet.size()) && rightSet.containsAll(leftSet);
    }

    HBaseTestingUtility utility = new HBaseTestingUtility();
    CitizenDAO citizenDao;
    CitizenSummaryDAO citizenSummaryDAO;
    private List<Citizen> testObjs = TestObjects.citizenList;

    @Before
    public void setup() throws Exception {
        utility.startMiniCluster();
        utility.createTable("citizens".getBytes(), new byte[][]{"main".getBytes(), "optional".getBytes()});
        utility.createTable("citizen_summary".getBytes(), new byte[][]{"a".getBytes()});
        Configuration configuration = utility.getConfiguration();
        citizenDao = new CitizenDAO(configuration) {
        };
        citizenSummaryDAO = new CitizenSummaryDAO(configuration) {
        };
    }

    @Test
    public void testTableParticulars() {
        assertEquals(citizenDao.getTableName(), "citizens");
        assertEquals(citizenSummaryDAO.getTableName(), "citizen_summary");
        assertTrue(setEquals(citizenDao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
        assertTrue(setEquals(citizenSummaryDAO.getColumnFamilies(), Sets.newHashSet("a")));
    }

    @Test
    public void testHBaseDAO() throws Exception {
        for (Citizen e : testObjs) {
            String rowKey = citizenDao.persist(e);
            Citizen pe = citizenDao.get(rowKey);
            assertEquals("Entry got corrupted upon persisting and fetching back", pe, e);
        }
    }

    @After
    public void tearDown() throws Exception {
        utility.shutdownMiniCluster();
    }
}
