package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

import java.util.Objects;

@SuppressWarnings("unused")
@HBTable(name = "employees", families = {@Family(name = "a")})
public class Employee extends AbstractRecord implements HBRecord<Long> {
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

    public Employee(Long empid, String empName, Short reporteeCount, Long createdAt) {
        this.empid = empid;
        this.empName = empName;
        this.reporteeCount = reporteeCount;
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee other = (Employee) o;
        return Objects.equals(empid, other.empid) &&
                Objects.equals(empName, other.empName) &&
                Objects.equals(reporteeCount, other.reporteeCount) &&
                Objects.equals(createdAt, other.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(empid, empName, reporteeCount, createdAt);
    }

    @Override
    public String toString() {
        return String.format("Employee{empid=%d, empName='%s', reporteeCount=%s, createdAt=%d}", empid, empName, reporteeCount, createdAt);
    }
}
