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
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class IcebergGlueMetaStorePropertiesTest {

    @Test
    public void glueTest() throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "glue",
                "glue.region", "us-west-2",
                "glue.access_key", "AK",
                "glue.secret_key", "SK",
                "glue.endpoint", "https://glue.us-west-2.amazonaws.com",
                "warehouse", "s3://my-bucket/warehouse");
        IcebergGlueMetaStoreProperties properties = (IcebergGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        Assertions.assertEquals("glue", properties.getIcebergCatalogType());
        Catalog catalog = properties.initializeCatalog("iceberg_catalog", StorageProperties.createAll(baseProps));
        Assertions.assertEquals(GlueCatalog.class, catalog.getClass());
    }

    @Test
    public void glueAndS3Test() throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "glue",
                "glue.region", "us-west-2",
                "glue.access_key", "AK",
                "glue.secret_key", "SK",
                "glue.endpoint", "https://glue.us-west-2.amazonaws.com",
                "warehouse", "s3://my-bucket/warehouse",
                "s3.region", "us-west-2",
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com"
        );
        IcebergGlueMetaStoreProperties properties = (IcebergGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        Catalog catalog = properties.initializeCatalog("iceberg_catalog", StorageProperties.createAll(baseProps));
        Assertions.assertEquals(GlueCatalog.class, catalog.getClass());
    }
}
