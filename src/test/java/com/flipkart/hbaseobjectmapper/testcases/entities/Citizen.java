package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

@SuppressWarnings("unused")
@ToString
@EqualsAndHashCode
@HBTable(name = "citizens", families = {@Family(name = "main"), @Family(name = "optional", versions = 10)})
public class Citizen implements HBRecord<String> {
    private static final String ROWKEY_DELIMITER = "#";
    @HBRowKey
    private String countryCode;
    @HBRowKey
    private Integer uid;
    @HBColumn(family = "main", column = "name")
    private String name;
    private transient String nameInUpper;
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
    @HBColumn(family = "optional", column = "pincode", codecFlags = {@Flag(name = BestSuitCodec.SERIALISE_AS_STRING, value = "true")})
    private Integer pincode;
    @HBColumnMultiVersion(family = "optional", column = "phone_number")
    private NavigableMap<Long, Integer> phoneNumber;
    @HBColumn(family = "optional", column = "codecFlags")
    private Map<String, Integer> extraFlags;
    @HBColumn(family = "optional", column = "dependents")
    private Dependents dependents; // Your own class
    @HBColumn(family = "optional", column = "emergency_contacts_1")
    private List<Contact> emergencyContacts1;
    @HBColumn(family = "optional", column = "emergency_contacts_2")
    private Map<String, Contact> emergencyContacts2;

    public Citizen() {
    }

    public Citizen(String countryCode, Integer uid, String name, Short age, Integer sal, Boolean isPassportHolder, Float f1, Double f2, Long f3, BigDecimal f4, Integer pincode, NavigableMap<Long, Integer> phoneNumber, Map<String, Integer> extraFlags, Dependents dependents, Map<String, Contact> emergencyContacts) {
        this.countryCode = countryCode;
        this.uid = uid;
        this.name = name;
        this.phoneNumber = phoneNumber;
        this.extraFlags = extraFlags;
        this.dependents = dependents;
        this.nameInUpper = name == null ? null : name.toUpperCase();
        this.age = age;
        this.sal = sal;
        this.isPassportHolder = isPassportHolder;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.pincode = pincode;
        emergencyContacts2 = emergencyContacts;
        if (emergencyContacts != null && !emergencyContacts.isEmpty()) {
            emergencyContacts1 = new ArrayList<>();
            for (Map.Entry<String, Contact> entry : emergencyContacts.entrySet()) {
                emergencyContacts1.add(entry.getValue());
            }
        }
    }

    @Override
    public String composeRowKey() {
        return String.format("%s%s%d", countryCode, ROWKEY_DELIMITER, uid);
    }

    @Override
    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split(ROWKEY_DELIMITER);
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

    public Map<String, Integer> getExtraFlags() {
        return extraFlags;
    }

    public Dependents getDependents() {
        return dependents;
    }

    public NavigableMap<Long, Integer> getPhoneNumber() {
        return phoneNumber;
    }

    public Boolean getPassportHolder() {
        return isPassportHolder;
    }

    public List<Contact> getEmergencyContacts1() {
        return emergencyContacts1;
    }

    public Map<String, Contact> getEmergencyContacts2() {
        return emergencyContacts2;
    }
}
