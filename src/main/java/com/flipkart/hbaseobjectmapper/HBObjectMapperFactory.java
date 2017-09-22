package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

/**
 * Maintains one instance of {@link HBObjectMapper} class. For internal use only.
 */
class HBObjectMapperFactory {
    /**
     * Default instance
     */
    private static HBObjectMapper hbObjectMapper = new HBObjectMapper();

    static HBObjectMapper construct(Codec codec) {
        return codec == null ? hbObjectMapper : new HBObjectMapper(codec);
    }
}

