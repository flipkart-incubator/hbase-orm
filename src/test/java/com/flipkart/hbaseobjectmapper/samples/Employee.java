package com.flipkart.hbaseobjectmapper.samples;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
@EqualsAndHashCode
@HBTable("employees")
public class Employee implements HBRecord {
    private String countryCode;
    private Integer empId;
    @HBColumn(family = "main", column = "name")
    private String name;
    @HBColumn(family = "optional", column = "age")
    private Short age;
    @HBColumn(family = "optional", column = "salary")
    private Integer sal;
    @HBColumn(family = "optional", column = "fte")
    private Boolean isFullTime;
    @HBColumn(family = "optional", column = "f1")
    private Float f1;
    @HBColumn(family = "optional", column = "f2")
    private Double f2;
    @HBColumn(family = "optional", column = "f3")
    private Long f3;
    @HBColumn(family = "optional", column = "f4")
    private BigDecimal f4;
    @HBColumn(family = "optional", column = "pincode", serializeAsString = true)
    private Integer pincode;

    public Employee() {
    }

    public Employee(String countryCode, Integer empId, String name, Short age, Integer sal, Boolean isFullTime, Float f1, Double f2, Long f3, BigDecimal f4, Integer pincode) {
        this.countryCode = countryCode;
        this.empId = empId;
        this.name = name;
        this.age = age;
        this.sal = sal;
        this.isFullTime = isFullTime;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.pincode = pincode;
    }

    public String composeRowKey() {
        return String.format("%s#%d", countryCode, empId);
    }

    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split("#");
        this.countryCode = pieces[0];
        this.empId = Integer.parseInt(pieces[1]);
    }
}
