package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.exceptions.AllHBColumnFieldsNullException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty;
import com.flipkart.hbaseobjectmapper.testcases.entities.*;
import org.javatuples.Triplet;

import java.math.BigDecimal;
import java.util.*;

import static com.flipkart.hbaseobjectmapper.testcases.util.LiteralsUtil.triplet;

public class TestObjects {
    public static final List<Citizen> validCitizenObjectsNoVersion = Arrays.asList(
            new Citizen("IND", 101, "Manu", (short) 30, 30000, false, 2.3f, 4.33, 34L, new BigDecimal(100), 560034, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis(), 100001);
                }
            }, new HashMap<String, Integer>() {
                {
                    put("a", 1);
                    put("b", 1);
                }
            }, new Dependents(121, Arrays.asList(122, 123)), new HashMap<String, Contact>() {
                {
                    put("spouse", new Contact("ABCD", 8888888));
                    put("father", new Contact("XYZ", 89898));
                }
            }),
            new Citizen("IND", 102, "Sathish", (short) 28, null, false, 2.3f, 4.33, -34L, new BigDecimal(100), 560034, null, new HashMap<String, Integer>(), new Dependents(131, null), null),
            new Citizen("IND", 103, "Akshay", (short) -1, 50000, false, null, 4.33e34, 34L, null, 560034, null, null, null, null),
            new Citizen("IND", 104, "Ananda", (short) 5, 30000, null, -2.3f, 4.33, 34L, new BigDecimal(1e50), 560034, null, null, new Dependents(), null),
            new Citizen("IND", 105, "Nilesh", null, null, null, null, null, null, null, null, null, null, new Dependents(null, Arrays.asList(141, 142)), null)
    );

    public static final List<HBRecord> validEmployeeObjects = asList(
            new Employee(1L, "Raja", (short) 0, System.currentTimeMillis()),
            new Employee(2L, "Ramnik", (short) 8, System.currentTimeMillis())
    );

    public static final List<HBRecord> validStudentObjects = asList(
            new Student(1, "Ishan"),
            new Student(2, "Akshit")
    );

    private static List<HBRecord> asList(HBRecord... hbRecords) {
        List<HBRecord> output = new ArrayList<>();
        Collections.addAll(output, hbRecords);
        return output;
    }

    public static final List<Citizen> validCitizenObjectsWithHBColumnMultiVersion = Arrays.asList(
            new Citizen("IND", 106, "Ram", null, 30000, true, null, null, null, null, null, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis() - 365L * 86400L * 1000L, 20000); // last year
                    put(System.currentTimeMillis() - 30L * 86400L * 1000L, 20001); // last month
                    put(System.currentTimeMillis(), 20002);
                }
            }, null, null, null),
            new Citizen("IND", 107, "Laxman", null, 35000, false, null, null, null, null, null, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis(), 20001);
                }
            }, null, null, null)
    );

    public static final List<Citizen> validCitizenObjects = new ArrayList<Citizen>() {
        {
            addAll(TestObjects.validCitizenObjectsNoVersion);
            addAll(TestObjects.validCitizenObjectsWithHBColumnMultiVersion);
        }
    };

    public static final List<HBRecord> validObjects = new ArrayList<HBRecord>() {
        {
            addAll(TestObjects.validCitizenObjects);
            addAll(TestObjects.validEmployeeObjects);
            addAll(TestObjects.validStudentObjects);
        }
    };

    @SuppressWarnings("unchecked")
    public static final List<Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidObjects = Arrays.asList(
            triplet(new Citizen("IND", -1, null, null, null, null, null, null, null, null, null, null, null, null, null), "all fields empty", AllHBColumnFieldsNullException.class),
            triplet(new Citizen("IND", 1, "row key", null, null, null, null, null, null, null, null, new TreeMap<Long, Integer>(), null, null, null), "an empty field annotated with @" + HBColumnMultiVersion.class.getName(), FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty.class)
    );
}
