package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@SuppressWarnings("unused")
@ToString
@EqualsAndHashCode
@HBTable(name = "employees", families = {@Family(name = "a")})
public class Employee implements HBRecord<Long> {
    @HBRowKey
    private Long empid;

    @HBColumn(family = "a", column = "name")
    private String empName;

    @HBColumn(family = "a", column = "reportee_count")
    private Short reporteeCount;

    @Override
    public Long composeRowKey() {
        return empid;
    }

    @Override
    public void parseRowKey(Long rowKey) {
        empid = rowKey;
    }

    public Long getEmpid() {
        return empid;
    }

    public Short getReporteeCount() {
        return reporteeCount;
    }

    public Employee() {

    }

    public Employee(Long empid, String empName, Short reporteeCount) {
        this.empid = empid;
        this.empName = empName;
        this.reporteeCount = reporteeCount;
    }
}
