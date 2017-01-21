package com.flipkart.hbaseobjectmapper;


import com.flipkart.hbaseobjectmapper.exceptions.BothHBColumnAnnotationsPresentException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations (internal use only)
 */
class WrappedHBColumn {
    private String family, column;
    private boolean multiVersioned = false, singleVersioned = false;
    private Class annotationClass;
    private Map<String, String> codecFlags;

    WrappedHBColumn(Field field) {
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);
        if (hbColumn != null && hbColumnMultiVersion != null) {
            throw new BothHBColumnAnnotationsPresentException(field);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            singleVersioned = true;
            annotationClass = HBColumn.class;
            codecFlags = toMap(hbColumn.codecFlags());
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
            codecFlags = toMap(hbColumnMultiVersion.codecFlags());
        }
    }

    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>();
        for (Flag flag : codecFlags) {
            flagsMap.put(flag.name(), flag.value());
        }
        return flagsMap;
    }

    public String family() {
        return family;
    }

    public String column() {
        return column;
    }

    public Map<String, String> codecFlags() {
        return codecFlags;
    }

    public boolean isPresent() {
        return singleVersioned || multiVersioned;
    }

    public boolean isMultiVersioned() {
        return multiVersioned;
    }

    public boolean isSingleVersioned() {
        return singleVersioned;
    }

    public String getName() {
        return annotationClass.getName();
    }
}
