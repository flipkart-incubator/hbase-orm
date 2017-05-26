package com.flipkart.hbaseobjectmapper.testcases.entities;


import com.flipkart.hbaseobjectmapper.*;
import lombok.ToString;

@ToString
@HBTable(name = "citizens_summary", families = {@Family(name = "a")})
public class CitizenSummary implements HBRecord<String> {

    @HBRowKey
    private String key;

    @HBColumn(family = "a", column = "average_age")
    private Float averageAge;

    public CitizenSummary() {
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

    public Float getAverageAge() {
        return averageAge;
    }

    public void setAverageAge(float averageAge) {
        this.averageAge = averageAge;
    }
}
