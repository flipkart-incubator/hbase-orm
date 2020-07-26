package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import lombok.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@SuppressWarnings("unused")
@HBTable(name = "corp:employees", families = {@Family(name = "a")})
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class Employee extends AbstractRecord {
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

    public Employee(Long empid, String empName, Short reporteeCount, Long createdAt) {
        this.empid = empid;
        this.empName = empName;
        this.reporteeCount = reporteeCount;
        this.createdAt = LocalDateTime.ofEpochSecond(createdAt, 0, ZoneOffset.UTC);
    }

}
