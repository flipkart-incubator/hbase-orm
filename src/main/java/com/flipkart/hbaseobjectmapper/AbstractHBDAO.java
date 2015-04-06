package com.flipkart.hbaseobjectmapper;

import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public abstract class AbstractHBDAO<T extends HBRecord> implements HBDAO {

    protected final HBObjectMapper hbObjectMapper = new HBObjectMapper();
    protected final Class<T> hbRecordClass;
    protected final HTable hTable;
    private TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };

    protected AbstractHBDAO(Configuration conf) throws IOException {
        hbRecordClass = (Class<T>) typeToken.getRawType();
        if (hbRecordClass == null || hbRecordClass == HBRecord.class)
            throw new IllegalStateException("Unable to resolve HBase record type");
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        if (hbTable == null)
            throw new IllegalStateException(String.format("Type %s should be annotated with %s for use in class %s", hbRecordClass.getName(), HBTable.class.getName(), AbstractHBDAO.class.getName()));
        this.hTable = new HTable(conf, hbTable.value());
    }

    @Override
    public T get(String rowKey) throws IOException {
        Result result = this.hTable.get(new Get(Bytes.toBytes(rowKey)));
        return hbObjectMapper.readValue(rowKey, result, hbRecordClass);
    }

    @Override
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

    public Map<String, String> fetchColumnValues(String[] rowKeys, String family, String column) throws IOException {
        List<Get> gets = new ArrayList<Get>(rowKeys.length);
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            gets.add(get);
        }
        Result[] results = this.hTable.get(gets);
        Map<String, String> map = new HashMap<String, String>(rowKeys.length);
        for (Result result : results) {
            KeyValue kv = result.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column));
            if (kv == null)
                continue;
            map.put(Bytes.toString(kv.getRow()), Bytes.toString(kv.getValue()));
        }
        return map;
    }

    @Override
    public String getTableName() {
        HBTable hbTable = hbRecordClass.getAnnotation(HBTable.class);
        return hbTable.value();
    }

    @Override
    public Set<String> getColumnFamilies() {
        return hbObjectMapper.getColumnFamilies(hbRecordClass);
    }
}
