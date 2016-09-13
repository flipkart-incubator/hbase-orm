package com.flipkart.hbaseobjectmapper.entities;


import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.ToString;

@ToString
@HBTable("employees_summary")
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
