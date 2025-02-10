// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.property.metastore;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

public class AWSGluePropertiesTest {

    @Test
    public void testBasicProperties() {
        Map<String, String> props = Maps.newHashMap();
        props.put("glue.access_key", "test_access_key");
        props.put("glue.secret_key", "test_secret_key");
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");

        AWSGlueProperties glueProperties = (AWSGlueProperties) MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);

        Map<String, String> catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assert.assertEquals("com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x", catalogProps
                .get("client.credentials-provider"));
        Assert.assertEquals(props.get("glue.access_key"), catalogProps
                .get("client.credentials-provider.glue.access_key"));
        Assert.assertEquals(props.get("glue.secret_key"), catalogProps
                .get("client.credentials-provider.glue.secret_key"));
        Assert.assertEquals("ap-northeast-1", catalogProps
                .get("client.region"));
        //Test glue.endpoint
        props = new HashMap<>();
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");
        props.put("aws.glue.secret-key", "test_secret_key");
        props.put("aws.glue.access-key", "test_access_key");
        glueProperties = (AWSGlueProperties) MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("ap-northeast-1", catalogProps.get("client.region"));
        Assertions.assertEquals("test_access_key", catalogProps.get("client.credentials-provider.glue.access_key"));
        Assertions.assertEquals("test_secret_key", catalogProps.get("client.credentials-provider.glue.secret_key"));
        props = new HashMap<>();
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");
        props.put("aws.glue.secret-key", "test_secret_key");
        props.put("glue.access_key", "test_glue_access_key");
        glueProperties = (AWSGlueProperties) MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("ap-northeast-1", catalogProps.get("client.region"));
        Assertions.assertEquals("test_secret_key", catalogProps.get("client.credentials-provider.glue.secret_key"));
        Assertions.assertEquals("test_glue_access_key", catalogProps.get("client.credentials-provider.glue.access_key"));
    }

    @Test
    public void testMissingRequiredProperties() {
        Map<String, String> props = Maps.newHashMap();
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        });
        props.put("glue.access_key", "test_access_key");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        });
        props.put("glue.secret_key", "test_secret_key");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        });
    }

    @Test
    public void testEmptyRequiredProperty() {
        Map<String, String> props = Maps.newHashMap();
        props.put("glue.access_key", " ");
        props.put("glue.secret_key", "test_secret_key");
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");

        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
        props.put("glue.access_key", "");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
        props.put("glue.access_key", "test_access_key");
        props.put("glue.secret_key", " ");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(MetastoreProperties.Type.GLUE, props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
    }

    @Test
    public void testNonRequiredProperties() {
        //None required properties
    }
}
