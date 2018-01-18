package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.NavigableMap;
import java.util.TreeMap;

@SuppressWarnings("unused")
@ToString
@EqualsAndHashCode
@HBTable(name = "counters", families = {@Family(name = "a", versions = 10)})
public class Counter implements HBRecord<String> {
    @HBRowKey
    private String key;

    @HBColumnMultiVersion(family = "a", column = "value")
    private NavigableMap<Long, Long> value;

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        key = rowKey;
    }

    public Counter() {
        value = new TreeMap<>();
    }

    public NavigableMap<Long, Long> getValue() {
        return value;
    }

    public Counter(String key) {
        this();
        this.key = key;
    }

    public Counter(String key, NavigableMap<Long, Long> value) {
        this(key);
        this.value = value;
    }


    public void set(Long timestamp, Long value) {
        this.value.put(timestamp, value);
    }
}
