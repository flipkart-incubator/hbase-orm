package com.flipkart.hbaseobjectmapper;

import java.io.IOException;
import java.util.Set;

public interface HBDAO {
    HBRecord get(String rowKey) throws IOException;

    HBRecord[] get(String[] rowKeys) throws IOException;

    String persist(HBRecord obj) throws IOException;

    String getTableName();

    Set<String> getColumnFamilies();
}
