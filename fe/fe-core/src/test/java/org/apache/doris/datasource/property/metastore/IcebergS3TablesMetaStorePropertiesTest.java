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
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.Map;

public class IcebergS3TablesMetaStorePropertiesTest {

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

}
