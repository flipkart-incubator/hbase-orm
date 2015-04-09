package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.Citizen;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class TestObjects {
    public static final List<Citizen> citizenList = Arrays.asList(
            new Citizen("IND", 101, "Manu", (short) 30, 30000, false, 2.3f, 4.33, 34L, new BigDecimal(100), 560034),
            new Citizen("IND", 102, "Sathish", (short) 28, null, false, 2.3f, 4.33, -34L, new BigDecimal(100), 560034),
            new Citizen("IND", 103, "Akshay", (short) -1, 50000, false, null, 4.33e34, 34L, null, 560034),
            new Citizen("IND", 104, "Ananda", (short) 5, 30000, null, -2.3f, 4.33, 34L, new BigDecimal(1e50), 560034),
            new Citizen("IND", 105, "Nilesh", null, null, null, null, null, null, null, null)
    );
}
