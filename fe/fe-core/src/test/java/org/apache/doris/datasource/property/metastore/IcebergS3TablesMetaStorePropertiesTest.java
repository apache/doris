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
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class IcebergS3TablesMetaStorePropertiesTest {

    /**
     * Call private buildS3CatalogProperties to fill catalogProps without initializing S3TablesCatalog
     * (which requires warehouse/table bucket ARN and would throw ValidationException).
     */
    private static void buildS3CatalogProperties(IcebergS3TablesMetaStoreProperties metaProps,
            Map<String, String> catalogProps) throws Exception {
        Method m = IcebergS3TablesMetaStoreProperties.class.getDeclaredMethod("buildS3CatalogProperties", Map.class);
        m.setAccessible(true);
        m.invoke(metaProps, catalogProps);
    }

    @Test
    public void s3TablesTest() throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "s3tables",
                "warehouse", "s3://my-bucket/warehouse");
        Map<String, String> s3Props = ImmutableMap.of(
                "s3.region", "us-west-2",
                "s3.access_key", "AK",
                "s3.secret_key", "SK",
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> MetastoreProperties.create(baseProps));
        Assertions.assertTrue(exception.getMessage().contains("Region is not set."));
        Map<String, String> allProps = ImmutableMap.<String, String>builder()
                .putAll(baseProps)
                .putAll(s3Props)
                .build();
        IcebergS3TablesMetaStoreProperties properties = (IcebergS3TablesMetaStoreProperties) MetastoreProperties.create(allProps);
        Catalog catalog = properties.initializeCatalog("iceberg_catalog", StorageProperties.createAll(allProps));
        Assertions.assertEquals(S3TablesCatalog.class, catalog.getClass());
    }

    @Test
    public void s3TablesWithIamRole() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "s3tables");
        props.put("warehouse", "s3://my-bucket/warehouse");
        props.put("s3.region", "us-east-1");
        props.put("s3.role_arn", "arn:aws:iam::123456789012:role/S3TablesRole");
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");

        IcebergS3TablesMetaStoreProperties metaProps = new IcebergS3TablesMetaStoreProperties(props);
        metaProps.initNormalizeAndCheckProps();

        Assertions.assertEquals(IcebergExternalCatalog.ICEBERG_S3_TABLES, metaProps.getIcebergCatalogType());

        Map<String, String> catalogProps = new HashMap<>();
        buildS3CatalogProperties(metaProps, catalogProps);

        Assertions.assertTrue(catalogProps.containsKey("client.factory"));
        Assertions.assertEquals("arn:aws:iam::123456789012:role/S3TablesRole", catalogProps.get("client.assume-role.arn"));
        Assertions.assertEquals("us-east-1", catalogProps.get("client.assume-role.region"));
    }

    @Test
    public void s3TablesWithIamRoleAndExternalId() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "s3tables");
        props.put("warehouse", "s3://my-bucket/warehouse");
        props.put("s3.region", "us-west-2");
        props.put("s3.role_arn", "arn:aws:iam::999999999999:role/MyRole");
        props.put("s3.external_id", "external-id-123");
        props.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");

        IcebergS3TablesMetaStoreProperties metaProps = new IcebergS3TablesMetaStoreProperties(props);
        metaProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = new HashMap<>();
        buildS3CatalogProperties(metaProps, catalogProps);

        Assertions.assertEquals("arn:aws:iam::999999999999:role/MyRole", catalogProps.get("client.assume-role.arn"));
        Assertions.assertEquals("external-id-123", catalogProps.get("client.assume-role.external-id"));
    }

    @Test
    public void s3TablesWithAccessKeyPreferOverIamRole() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "s3tables");
        props.put("warehouse", "s3://my-bucket/warehouse");
        props.put("s3.region", "us-east-1");
        props.put("s3.access_key", "AKID");
        props.put("s3.secret_key", "SECRET");
        props.put("s3.role_arn", "arn:aws:iam::123456789012:role/Role");
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");

        IcebergS3TablesMetaStoreProperties metaProps = new IcebergS3TablesMetaStoreProperties(props);
        metaProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = new HashMap<>();
        buildS3CatalogProperties(metaProps, catalogProps);

        Assertions.assertTrue(catalogProps.containsKey("client.credentials-provider"));
        Assertions.assertEquals("AKID", catalogProps.get("client.credentials-provider.s3.access-key-id"));
        Assertions.assertEquals("SECRET", catalogProps.get("client.credentials-provider.s3.secret-access-key"));
        Assertions.assertNull(catalogProps.get("client.assume-role.arn"));
    }
}
