package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

public class HBObjectMapperFactoryTC extends HBObjectMapperFactory {
    public static HBObjectMapper construct(Codec codec) {
        return HBObjectMapperFactory.construct(codec);
    }
}
