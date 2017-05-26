package com.flipkart.hbaseobjectmapper.testcases.entities;


import com.flipkart.hbaseobjectmapper.*;
import lombok.ToString;

@ToString
@HBTable(name = "employees_summary", families = {@Family(name = "a")})
public class EmployeeSummary implements HBRecord<String> {

    @HBRowKey
    private String key;

    @HBColumn(family = "a", column = "average_salary")
    private Float averageReporteeCount;

    public EmployeeSummary() {
        key = "summary";
    }

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        key = rowKey;
    }

    public Float getAverageReporteeCount() {
        return averageReporteeCount;
    }

    public void setAverageReporteeCount(Float averageReporteeCount) {
        this.averageReporteeCount = averageReporteeCount;
    }
}
