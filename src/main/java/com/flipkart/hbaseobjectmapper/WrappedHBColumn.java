package com.flipkart.hbaseobjectmapper;


import com.flipkart.hbaseobjectmapper.exceptions.BothHBColumnAnnotationsPresentException;

import java.lang.reflect.Field;

/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations
 */
class WrappedHBColumn {
    private String family, column;
    private boolean serializeAsString = false, multiVersioned = false, singleVersioned = false;
    private Class annotationClass;

    public WrappedHBColumn(Field field) {
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);
        if (hbColumn != null && hbColumnMultiVersion != null) {
            throw new BothHBColumnAnnotationsPresentException(field);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            serializeAsString = hbColumn.serializeAsString();
            singleVersioned = true;
            annotationClass = HBColumn.class;
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            serializeAsString = hbColumnMultiVersion.serializeAsString();
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
        }
    }

    public String family() {
        return family;
    }

    public String column() {
        return column;
    }

    public boolean serializeAsString() {
        return serializeAsString;
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
