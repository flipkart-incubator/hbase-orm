package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

/**
 * Maintains one instance of {@link HBObjectMapper} class. For internal use only.
 */
class HBObjectMapperFactory {
    private HBObjectMapperFactory() {
        throw new UnsupportedOperationException();
    }

    /**
     * Default instance of {@link HBObjectMapper}
     */
    private static HBObjectMapper hbObjectMapper;
    private static final Object[] lock = new Object[0];

    static HBObjectMapper construct(Codec codec) {
        if (hbObjectMapper == null) {
            synchronized (lock) {
                hbObjectMapper = codec == null ? new HBObjectMapper() : new HBObjectMapper(codec);
            }
        }
        return hbObjectMapper;
    }
}

