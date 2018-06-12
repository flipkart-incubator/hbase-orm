package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.util.Pair;

/**
 * A tuple of family name and column name. For internal use only.
 */
class FamilyAndColumn extends Pair<String, String> {
    FamilyAndColumn(String family, String column) {
        super(family, column);
    }
}
