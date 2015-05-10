package com.flipkart.hbaseobjectmapper;

import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;

/**
 * A <i>Data Access Object</i> class that enables simpler random access of HBase rows
 *
 * @param <T> The entity that maps to an HBase row
 */
public abstract class AbstractHBDAO<T extends HBRecord> {

    protected final HBObjectMapper hbObjectMapper = new HBObjectMapper();
    protected final Class<T> hbRecordClass;
    protected final HTable hTable;
    private TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    protected Map<String, Field> fields;

    protected AbstractHBDAO(Configuration conf) throws IOException {
        hbRecordClass = (Class<T>) typeToken.getRawType();
        if (hbRecordClass == null || hbRecordClass == HBRecord.class)
            throw new IllegalStateException("Unable to resolve HBase record type");
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        if (hbTable == null)
            throw new IllegalStateException(String.format("Type %s should be annotated with %s for use in class %s", hbRecordClass.getName(), HBTable.class.getName(), AbstractHBDAO.class.getName()));
        this.hTable = new HTable(conf, hbTable.value());
        this.fields = hbObjectMapper.getHBFields(hbRecordClass);
    }

    public T get(String rowKey) throws IOException {
        Result result = this.hTable.get(new Get(Bytes.toBytes(rowKey)));
        return hbObjectMapper.readValue(rowKey, result, hbRecordClass);
    }

    public T[] get(String[] rowKeys) throws IOException {
        List<Get> gets = new ArrayList<Get>(rowKeys.length);
        for (String rowKey : rowKeys) {
            gets.add(new Get(Bytes.toBytes(rowKey)));
        }
        Result[] results = this.hTable.get(gets);
        @SuppressWarnings("unchecked") T[] records = (T[]) Array.newInstance(hbRecordClass, rowKeys.length);
        for (int i = 0; i < records.length; i++) {
            records[i] = hbObjectMapper.readValue(rowKeys[i], results[i], hbRecordClass);
        }
        return records;
    }

    public String persist(HBRecord obj) throws IOException {
        Put put = hbObjectMapper.writeValueAsPut(obj);
        hTable.put(put);
        return obj.composeRowKey();
    }


    public void delete(String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        this.hTable.delete(delete);
    }

    public void delete(HBRecord obj) throws IOException {
        this.delete(obj.composeRowKey());
    }

    public void delete(String[] rowKeys) throws IOException {
        List<Delete> deletes = new ArrayList<Delete>(rowKeys.length);
        for (String rowKey : rowKeys) {
            deletes.add(new Delete(Bytes.toBytes(rowKey)));
        }
        this.hTable.delete(deletes);
    }

    public void delete(HBRecord[] objs) throws IOException {
        String[] rowKeys = new String[objs.length];
        for (int i = 0; i < objs.length; i++) {
            rowKeys[i] = objs[i].composeRowKey();
        }
        this.delete(rowKeys);
    }

    public String getTableName() {
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        return hbTable.value();
    }

    public Set<String> getColumnFamilies() {
        return hbObjectMapper.getColumnFamilies(hbRecordClass);
    }

    public Set<String> getFields() {
        return fields.keySet();
    }


    public HTable getHBaseTable() {
        return hTable;
    }

    public Object fetchFieldValue(String rowKey, String fieldName) throws IOException {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Unrecognized field: '%s'. Choose one of %s", fieldName, fields.values().toString()));
        }
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
        Result result = this.hTable.get(get);
        KeyValue kv = result.getColumnLatest(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
        if (kv == null)
            return null;
        return hbObjectMapper.toFieldValue(kv.getValue(), field);
    }

    public Map<String, Object> fetchFieldValues(String[] rowKeys, String fieldName) throws IOException {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Unrecognized field: '%s'. Choose one of %s", fieldName, fields.values().toString()));
        }
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        List<Get> gets = new ArrayList<Get>(rowKeys.length);
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
            gets.add(get);
        }
        Result[] results = this.hTable.get(gets);
        Map<String, Object> map = new HashMap<String, Object>(rowKeys.length);
        for (Result result : results) {
            KeyValue kv = result.getColumnLatest(Bytes.toBytes(hbColumn.family()), Bytes.toBytes(hbColumn.column()));
            if (kv == null)
                continue;
            map.put(Bytes.toString(kv.getRow()), hbObjectMapper.toFieldValue(kv.getValue(), field));
        }
        return map;
    }
}
