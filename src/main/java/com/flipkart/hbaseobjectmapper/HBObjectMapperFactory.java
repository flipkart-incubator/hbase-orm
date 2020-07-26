package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains one instance of {@link HBObjectMapper} class. For internal use only.
 */
class HBObjectMapperFactory {
    /**
     * Default instance of {@link HBObjectMapper}
     */
    private static HBObjectMapper defaultHBObjectMapper;

    private static final Map<Class<? extends Codec>, HBObjectMapper> customHBObjectMappers = new HashMap<>();

    protected HBObjectMapperFactory() {
        throw new UnsupportedOperationException();
    }

    static synchronized HBObjectMapper construct() {
        if (defaultHBObjectMapper == null) {
            defaultHBObjectMapper = new HBObjectMapper();
        }
        return defaultHBObjectMapper;
    }

    static synchronized HBObjectMapper construct(Codec codec) {
        HBObjectMapper customHBObjectMapper = customHBObjectMappers.get(codec.getClass());
        if (customHBObjectMapper == null) {
            customHBObjectMapper = new HBObjectMapper(codec);
            customHBObjectMappers.put(codec.getClass(), customHBObjectMapper);
        }
        return customHBObjectMapper;
    }
}

