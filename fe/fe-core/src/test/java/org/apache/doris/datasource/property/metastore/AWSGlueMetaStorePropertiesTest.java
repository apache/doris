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

import org.apache.doris.common.UserException;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

@Disabled("wait metastore integration")
public class AWSGlueMetaStorePropertiesTest {
    private static Map<String, String> baseProps = new HashMap<>();

    @BeforeAll
    public static void setUp() {
        baseProps.put("iceberg.catalog.type", "glue");
        baseProps.put("type", "iceberg");
    }

    @Test
    public void testBasicProperties() throws UserException {
        Map<String, String> props = baseProps;
        props.put("glue.access_key", "test_access_key");
        props.put("glue.secret_key", "test_secret_key");

        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");

        AWSGlueMetaStoreProperties glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);

        Map<String, String> catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x", catalogProps
                .get("client.credentials-provider"));
        Assertions.assertEquals(props.get("glue.access_key"), catalogProps
                .get("client.credentials-provider.glue.access_key"));
        Assertions.assertEquals(props.get("glue.secret_key"), catalogProps
                .get("client.credentials-provider.glue.secret_key"));
        Assertions.assertEquals("ap-northeast-1", catalogProps
                .get("client.region"));
        AWSGlueMetaStoreProperties.AWSCatalogMetastoreClientCredentials awsCatalogMetastoreClientCredentials = glueProperties.getAWSCatalogMetastoreClientCredentials();
        Map<String, String> credentials = awsCatalogMetastoreClientCredentials.getCredentials();
        Assertions.assertEquals("test_access_key", credentials.get("aws.glue.access-key"));
        Assertions.assertEquals("test_secret_key", credentials.get("aws.glue.secret-key"));
        Assertions.assertEquals("https://glue.ap-northeast-1.amazonaws.com", credentials.get("aws.glue.endpoint"));
        //Test glue.endpoint
        props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.type", "glue");
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");
        props.put("aws.glue.secret-key", "test_secret_key");
        props.put("aws.glue.access-key", "test_access_key");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("ap-northeast-1", catalogProps.get("client.region"));
        Assertions.assertEquals("test_access_key", catalogProps.get("client.credentials-provider.glue.access_key"));
        Assertions.assertEquals("test_secret_key", catalogProps.get("client.credentials-provider.glue.secret_key"));
        props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.type", "glue");
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");
        props.put("aws.glue.secret-key", "test_secret_key");
        props.put("glue.access_key", "test_glue_access_key");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
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
            MetastoreProperties.create(props);
        });
        props.put("glue.access_key", "test_access_key");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(props);
        });
        props.put("glue.secret_key", "test_secret_key");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(props);
        });
    }

    @Test
    public void testEmptyRequiredProperty() {
        Map<String, String> props = Maps.newHashMap();
        props.put("glue.access_key", " ");
        props.put("glue.secret_key", "test_secret_key");
        props.put("glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");

        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
        props.put("glue.access_key", "");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
        props.put("glue.access_key", "test_access_key");
        props.put("glue.secret_key", " ");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            MetastoreProperties.create(props);
        }, "AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) are not set correctly.");
    }

    @Test
    public void testEndpointParams() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("glue.access_key", "a");
        props.put("glue.secret_key", "test_secret_key");
        props.put("glue.endpoint", "https://glue.us-west-2.amazonaws.com");
        AWSGlueMetaStoreProperties glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        Map<String, String> catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        props.put("glue.endpoint", "https://glue-fips.us-west-2.api.aws");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        props.put("glue.endpoint", "https://glue-fips.us-west-2.amazonaws.com");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        props.put("glue.endpoint", "https://glue.us-west-2.api.aws");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        props.put("glue.endpoint", "https://glue.us-west-2.amazonaws.com");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));

        props.put("glue.endpoint", "glue.us-west-2.amazonaws.com");
        glueProperties = (AWSGlueMetaStoreProperties) MetastoreProperties.create(props);
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);
        Assertions.assertEquals("us-west-2", catalogProps.get("client.region"));
        props.put("glue.endpoint", "https://glue.us-west-2.amaaws.com");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(props), "AWS Glue properties (glue.endpoint) are not set correctly: https://glue.us-west-2.amaaws.com");


    }
}
