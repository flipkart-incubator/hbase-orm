package com.flipkart.hbaseobjectmapper.testcases;

import com.flipkart.hbaseobjectmapper.HBAdmin;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.WrappedHBColumnTC;
import com.flipkart.hbaseobjectmapper.WrappedHBTableTC;
import com.flipkart.hbaseobjectmapper.codec.JavaObjectStreamCodec;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import com.flipkart.hbaseobjectmapper.testcases.util.cluster.HBaseCluster;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@SuppressWarnings({"unchecked", "rawtypes"})
abstract class BaseHBDAOTests {

    protected static HBAdmin hbAdmin;
    protected static HBaseCluster hBaseCluster;

    protected static <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTables(Class... classes) throws IOException {
        for (Class<T> clazz : classes) {
            WrappedHBTableTC<R, T> hbTable = new WrappedHBTableTC<>(clazz);
            if (hbAdmin.tableExists(clazz)) {
                System.out.format("Deleting table '%s': ", hbTable);
                hbAdmin.disableTable(clazz);
                hbAdmin.deleteTable(clazz);
                System.out.println("[DONE]");
            }
        }
    }

    protected static <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTables(Class... classes) throws IOException {
        for (Class<T> clazz : classes) {
            WrappedHBTableTC<R, T> hbTable = new WrappedHBTableTC<>(clazz);
            System.out.format("Creating table '%s': ", hbTable);
            hbAdmin.createTable(clazz);
            System.out.println("[DONE]");
        }
    }

    protected <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T pruneVersionsBeyond(T record, int versions) {
        try {
            T prunedRecord = JavaObjectStreamCodec.deepCopy(record);
            for (Field field : Citizen.class.getDeclaredFields()) {
                WrappedHBColumnTC hbColumn = new WrappedHBColumnTC(field);
                if (hbColumn.isMultiVersioned()) {
                    field.setAccessible(true);
                    NavigableMap nm = (NavigableMap) field.get(prunedRecord);
                    if (nm == null || nm.isEmpty())
                        continue;
                    NavigableMap temp = new TreeMap<>();
                    for (int i = 0; i < versions; i++) {
                        final Map.Entry entry = nm.pollLastEntry();
                        if (entry == null)
                            break;
                        temp.put(entry.getKey(), entry.getValue());
                    }
                    field.set(prunedRecord, temp.isEmpty() ? null : temp);
                }
            }
            return prunedRecord;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
