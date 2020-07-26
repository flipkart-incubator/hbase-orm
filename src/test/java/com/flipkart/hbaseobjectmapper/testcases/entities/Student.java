package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.math.BigDecimal;

import static com.flipkart.hbaseobjectmapper.codec.BestSuitCodec.SERIALIZE_AS_STRING;

@SuppressWarnings("unused")
@ToString
@EqualsAndHashCode
@HBTable(name = "students", families = {@Family(name = "a")}, rowKeyCodecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
public class Student implements HBRecord<Integer> {
    private Integer studentId;

    @HBColumn(family = "a", column = "name")
    private String name;

    @HBColumn(family = "a", column = "f1", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Short f1;

    @HBColumn(family = "a", column = "f2", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Integer f2;

    @HBColumn(family = "a", column = "f3", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Long f3;

    @HBColumn(family = "a", column = "f4", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Float f4;

    @HBColumn(family = "a", column = "f5", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Double f5;

    @HBColumn(family = "a", column = "f6", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private BigDecimal f6;

    @HBColumn(family = "a", column = "f7", codecFlags = {@Flag(name = SERIALIZE_AS_STRING, value = "true")})
    private Boolean f7;

    @Override
    public Integer composeRowKey() {
        return studentId;
    }

    @Override
    public void parseRowKey(Integer rowKey) {
        studentId = rowKey;
    }

    public Integer getStudentId() {
        return studentId;
    }

    public Student() {

    }

    public Student(Integer studentId, String name, Short f1, Integer f2, Long f3, Float f4, Double f5, BigDecimal f6, Boolean f7) {
        this.studentId = studentId;
        this.name = name;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
        this.f7 = f7;
    }
}
