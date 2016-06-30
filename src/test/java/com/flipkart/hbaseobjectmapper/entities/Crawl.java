package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.ToString;

import java.util.NavigableMap;
import java.util.TreeMap;

@HBTable("crawl")
@ToString
public class Crawl implements HBRecord<String> {
    @HBRowKey
    String key;

    @HBColumnMultiVersion(family = "a", column = "f1")
    NavigableMap<Long, Double> f1;

    public Crawl() {

    }

    public Crawl(String key) {
        this.key = key;
        this.f1 = new TreeMap<Long, Double>();
    }

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    public Crawl addF1(Double f1) {
        this.f1.put(System.currentTimeMillis(), f1);
        return this;
    }

    public Crawl addF1(long timestamp, Double f1) {
        this.f1.put(timestamp, f1);
        return this;
    }

    public NavigableMap<Long, Double> getF1() {
        return f1;
    }
}
