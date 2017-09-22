package com.flipkart.hbaseobjectmapper;


import com.flipkart.hbaseobjectmapper.exceptions.BothHBColumnAnnotationsPresentException;
import com.flipkart.hbaseobjectmapper.exceptions.DuplicateCodecFlagForColumnException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations (for internal use only)
 */
class WrappedHBColumn {
    private final String family, column;
    private final boolean multiVersioned, singleVersioned;
    private final Class annotationClass;
    private final Map<String, String> codecFlags;
    private final Field field;

    WrappedHBColumn(Field field) {
        this.field = field;
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);
        if (hbColumn != null && hbColumnMultiVersion != null) {
            throw new BothHBColumnAnnotationsPresentException(field);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            singleVersioned = true;
            multiVersioned = false;
            annotationClass = HBColumn.class;
            codecFlags = toMap(hbColumn.codecFlags());
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            singleVersioned = false;
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
            codecFlags = toMap(hbColumnMultiVersion.codecFlags());
        } else {
            family = null;
            column = null;
            singleVersioned = false;
            multiVersioned = false;
            annotationClass = null;
            codecFlags = null;
        }
    }

    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>(codecFlags.length, 1.0f);
        for (Flag flag : codecFlags) {
            String previousValue = flagsMap.put(flag.name(), flag.value());
            if (previousValue != null) {
                throw new DuplicateCodecFlagForColumnException(field.getDeclaringClass(), field.getName(), annotationClass, flag.name());
            }
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

    @Override
    public String toString() {
        return String.format("%s:%s", family, column);
    }
}
