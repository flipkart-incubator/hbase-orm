package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.samples.Employee;
import com.flipkart.hbaseobjectmapper.samples.EmployeeDAO;
import com.flipkart.hbaseobjectmapper.samples.TestObjects;
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
    EmployeeDAO dao;
    private List<Employee> testObjs = TestObjects.TEST_OBJECTS;

    @Before
    public void setup() throws Exception {
        utility.startMiniCluster();
        utility.createTable("employees".getBytes(), new byte[][]{"main".getBytes(), "optional".getBytes()});
        Configuration configuration = utility.getConfiguration();
        dao = new EmployeeDAO(configuration) {
        };
    }

    @Test
    public void testTableParticulars() {
        assertEquals(dao.getTableName(), "employees");
        assertTrue(setEquals(dao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
    }

    @Test
    public void testHBaseDAO() throws Exception {
        for (Employee e : testObjs) {
            String rowKey = dao.persist(e);
            Employee pe = dao.get(rowKey);
            assertEquals("Entry got corrupted upon persisting and fetching back", pe, e);
        }
    }

    @After
    public void tearDown() throws Exception {
        utility.shutdownMiniCluster();
    }
}
