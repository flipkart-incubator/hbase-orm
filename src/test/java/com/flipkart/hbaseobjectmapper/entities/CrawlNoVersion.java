package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.ToString;

@HBTable("crawl")
@ToString
public class CrawlNoVersion implements HBRecord {
    @HBRowKey
    String key;

    @HBColumn(family = "a", column = "f1")
    Double f1;

    public CrawlNoVersion() {

    }

    public CrawlNoVersion(String key) {
        this.key = key;
    }

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    public Double getF1() {
        return f1;
    }

    public CrawlNoVersion setF1(Double f1) {
        this.f1 = f1;
        return this;
    }
}
