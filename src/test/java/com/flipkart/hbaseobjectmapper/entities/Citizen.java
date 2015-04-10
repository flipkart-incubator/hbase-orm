package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
@EqualsAndHashCode
@HBTable("citizens")
public class Citizen implements HBRecord {
    @HBRowKey
    private String countryCode;
    @HBRowKey
    private Integer uid;
    @HBColumn(family = "main", column = "name")
    private String name;
    @HBColumn(family = "optional", column = "age")
    private Short age;
    @HBColumn(family = "optional", column = "salary")
    private Integer sal;
    @HBColumn(family = "optional", column = "iph")
    private Boolean isPassportHolder;
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

    public Citizen() {
    }

    public Citizen(String countryCode, Integer uid, String name, Short age, Integer sal, Boolean isPassportHolder, Float f1, Double f2, Long f3, BigDecimal f4, Integer pincode) {
        this.countryCode = countryCode;
        this.uid = uid;
        this.name = name;
        this.age = age;
        this.sal = sal;
        this.isPassportHolder = isPassportHolder;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.pincode = pincode;
    }

    public String composeRowKey() {
        return String.format("%s#%d", countryCode, uid);
    }

    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split("#");
        this.countryCode = pieces[0];
        this.uid = Integer.parseInt(pieces[1]);
    }

    // Getter methods:

    public String getCountryCode() {
        return countryCode;
    }

    public Integer getUid() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public Integer getSal() {
        return sal;
    }

    public Boolean isPassportHolder() {
        return isPassportHolder;
    }

    public Float getF1() {
        return f1;
    }

    public Double getF2() {
        return f2;
    }

    public Long getF3() {
        return f3;
    }

    public BigDecimal getF4() {
        return f4;
    }

    public Integer getPincode() {
        return pincode;
    }

    public Short getAge() {
        return age;
    }
}
