package org.apache.doris.datasource.property.metastore;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HMSPropertiesTest {
    
    @Test
    public void testBasicParamsTest() {
        Map<String,String> params =new HashMap<>();
        params.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
 
        HMSProperties hmsProperties = new HMSProperties(params);
        hmsProperties.checkRequiredProperties();
    }
}
