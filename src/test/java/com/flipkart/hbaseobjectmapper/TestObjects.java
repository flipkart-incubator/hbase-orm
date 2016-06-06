package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.flipkart.hbaseobjectmapper.entities.Dependents;
import com.flipkart.hbaseobjectmapper.exceptions.AllHBColumnFieldsNullException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty;
import com.flipkart.hbaseobjectmapper.exceptions.HBRowKeyFieldCantBeNullException;
import org.javatuples.Triplet;

import java.math.BigDecimal;
import java.util.*;

import static com.flipkart.hbaseobjectmapper.TestUtil.triplet;


public class TestObjects {
    public static final List<Citizen> validObjectsNoVersion = Arrays.asList(
            new Citizen("IND", 101, "Manu", (short) 30, 30000, false, 2.3f, 4.33, 34L, new BigDecimal(100), 560034, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis(), 100001);
                }
            }, new HashMap<String, Integer>() {
                {
                    put("a", 1);
                    put("b", 1);
                }
            }, new Dependents(121, Arrays.asList(122, 123))),
            new Citizen("IND", 102, "Sathish", (short) 28, null, false, 2.3f, 4.33, -34L, new BigDecimal(100), 560034, null, new HashMap<String, Integer>(), new Dependents(131, null)),
            new Citizen("IND", 103, "Akshay", (short) -1, 50000, false, null, 4.33e34, 34L, null, 560034, null, null, null),
            new Citizen("IND", 104, "Ananda", (short) 5, 30000, null, -2.3f, 4.33, 34L, new BigDecimal(1e50), 560034, null, null, new Dependents()),
            new Citizen("IND", 105, "Nilesh", null, null, null, null, null, null, null, null, null, null, new Dependents(null, Arrays.asList(141, 142)))
    );

    public static final List<Citizen> validObjectsWithHBColumnMultiVersion = Arrays.asList(
            new Citizen("IND", 106, "Ram", null, 30000, true, null, null, null, null, null, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis() - 365L * 86400L * 1000L, 20000); // last year
                    put(System.currentTimeMillis() - 30L * 86400L * 1000L, 20001); // last month
                    put(System.currentTimeMillis(), 20002);
                }
            }, null, null),
            new Citizen("IND", 107, "Laxman", null, 35000, false, null, null, null, null, null, new TreeMap<Long, Integer>() {
                {
                    put(System.currentTimeMillis(), 20001);
                }
            }, null, null)
    );

    public static final List<Citizen> validObjects = new ArrayList<Citizen>() {
        {
            addAll(TestObjects.validObjectsNoVersion);
            addAll(TestObjects.validObjectsWithHBColumnMultiVersion);
        }
    };
    @SuppressWarnings("unchecked")
    public static final List<Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidObjects = Arrays.asList(
            triplet(new Citizen("IND", -1, null, null, null, null, null, null, null, null, null, null, null, null), "all fields empty", AllHBColumnFieldsNullException.class),
            triplet(new Citizen(null, -2, "row key field null 1", null, null, null, null, null, null, null, null, null, null, null), "one row key field null (variant 1)", HBRowKeyFieldCantBeNullException.class),
            triplet(new Citizen("IND", null, "row key field null 2", null, null, null, null, null, null, null, null, null, null, null), "one row key field null (variant 2)", HBRowKeyFieldCantBeNullException.class),
            triplet(new Citizen(null, null, "row key fields null", null, null, null, null, null, null, null, null, null, null, null), "all row key fields null", HBRowKeyFieldCantBeNullException.class),
            triplet(new Citizen("IND", 1, "row key", null, null, null, null, null, null, null, null, new TreeMap<Long, Integer>(), null, null), "an empty field annotated with @" + HBColumnMultiVersion.class.getName(), FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty.class)
    );
}
