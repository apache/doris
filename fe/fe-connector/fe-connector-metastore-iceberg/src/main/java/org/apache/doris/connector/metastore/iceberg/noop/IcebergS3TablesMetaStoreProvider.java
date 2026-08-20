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

package org.apache.doris.connector.metastore.iceberg.noop;

import org.apache.doris.connector.metastore.spi.MetaStoreProvider;

import java.util.Map;

/**
 * Selects the iceberg S3 Tables backend ({@code iceberg.catalog.type == s3tables}). No metastore rules
 * (storage validated upstream): binds a no-op-validate {@link IcebergNoOpMetaStoreProperties} so
 * {@code bindForType("s3tables")} resolves instead of throwing.
 */
public final class IcebergS3TablesMetaStoreProvider implements MetaStoreProvider<IcebergNoOpMetaStoreProperties> {

    @Override
    public boolean supportsType(String catalogType) {
        return "s3tables".equalsIgnoreCase(catalogType);
    }

    @Override
    public IcebergNoOpMetaStoreProperties bind(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        return IcebergNoOpMetaStoreProperties.of(properties, "S3TABLES");
    }

    @Override
    public String name() {
        return "S3TABLES";
    }
}
