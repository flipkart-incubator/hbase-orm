package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.exceptions.DuplicateCodecFlagForRowKeyException;
import com.flipkart.hbaseobjectmapper.exceptions.ImproperHBTableAnnotationExceptions;
import org.apache.hadoop.hbase.TableName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A wrapper for {@link HBTable} annotation (for internal use only)
 *
 * @param <R> Data type of row key
 * @param <T> Entity type
 */
class WrappedHBTable<R extends Serializable & Comparable<R>, T extends HBRecord<R>> {

    private final TableName tableName;
    private final Map<String, Integer> families; // This should evolve to Map<String, FamilyDetails>
    private final Map<String, String> codecFlags;
    private final Class<T> clazz;

    WrappedHBTable(Class<T> clazz) {
        this.clazz = clazz;
        final HBTable hbTable = clazz.getAnnotation(HBTable.class);
        if (hbTable == null) {
            throw new ImproperHBTableAnnotationExceptions.MissingHBTableAnnotationException(String.format("Class %s is missing %s annotation", clazz.getName(), HBTable.class.getSimpleName()));
        }
        if (hbTable.name().isEmpty()) {
            throw new ImproperHBTableAnnotationExceptions.EmptyTableNameOnHBTableAnnotationException(String.format("Annotation %s on class %s has empty name", HBTable.class.getName(), clazz.getName()));
        }
        tableName = TableName.valueOf(hbTable.name().getBytes());
        codecFlags = toMap(hbTable.rowKeyCodecFlags());
        families = new HashMap<>(hbTable.families().length, 1.0f);
        for (Family family : hbTable.families()) {
            if (family.name().isEmpty()) {
                throw new ImproperHBTableAnnotationExceptions.EmptyColumnFamilyOnHBTableAnnotationException(String.format("The %s annotation on class %s has a column family with empty name", HBTable.class.getSimpleName(), clazz.getName()));
            }
            if (family.versions() < 1) {
                throw new ImproperHBTableAnnotationExceptions.InvalidValueForVersionsOnHBTableAnnotationException(String.format("The %s annotation on class %s has a column family '%s' which has 'versions' less than 1", HBTable.class.getSimpleName(), clazz.getName(), family.name()));
            }
            final Integer prevValue = families.put(family.name(), family.versions());
            if (prevValue != null) {
                throw new ImproperHBTableAnnotationExceptions.DuplicateColumnFamilyNamesOnHBTableAnnotationException(String.format("The %s annotation on class %s has two or more column families with same name '%s' (Note: column family names must be unique)", HBTable.class.getSimpleName(), clazz.getName(), family.name()));
            }
        }
    }

    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>(codecFlags.length, 1.0f);
        for (Flag flag : codecFlags) {
            String previousValue = flagsMap.put(flag.name(), flag.value());
            if (previousValue != null) {
                throw new DuplicateCodecFlagForRowKeyException(clazz, flag.name());
            }
        }
        return flagsMap;
    }

    int getNumVersions(String familyName) {
        return families.get(familyName);
    }

    Map<String, Integer> getFamiliesAndVersions() {
        return families;
    }

    boolean isColumnFamilyPresent(String familyName) {
        return families.containsKey(familyName);
    }

    TableName getName() {
        return tableName;
    }

    public Map<String, String> getCodecFlags() {
        return codecFlags;
    }

    @Override
    public String toString() {
        return tableName.getNameAsString();
    }

    static <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, String> getCodecFlags(Class<T> clazz) {
        return new WrappedHBTable<>(clazz).codecFlags;
    }
}
