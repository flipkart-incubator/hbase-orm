package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.util.Pair;

class FamilyAndColumn extends Pair<String, String> {
    FamilyAndColumn(String family, String column) {
        super(family, column);
    }
}
