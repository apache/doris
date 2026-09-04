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

package org.apache.doris.connector.metastore.iceberg.jdbc;

import org.apache.doris.connector.metastore.spi.MetaStoreProvider;

import java.util.Map;

/** Selects the iceberg JDBC catalog backend ({@code iceberg.catalog.type == jdbc}). */
public final class IcebergJdbcMetaStoreProvider implements MetaStoreProvider<IcebergJdbcMetaStoreProperties> {

    @Override
    public boolean supportsType(String catalogType) {
        return "jdbc".equalsIgnoreCase(catalogType);
    }

    @Override
    public IcebergJdbcMetaStoreProperties bind(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        return IcebergJdbcMetaStoreProperties.of(properties);
    }

    @Override
    public String name() {
        return "JDBC";
    }
}
