package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

import java.util.Map;
import java.util.NavigableMap;

public class ClassesWithFieldIncompatibleWithHBColumnMultiVersion {
    public static class NotMap implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private Integer i;
    }

    public static class NotNavigableMap implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private Map<Long, Integer> i;
    }

    public static class EntryKeyNotLong implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private NavigableMap<Integer, Integer> i;
    }
}
