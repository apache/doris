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

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Shared no-op metastore backend for the iceberg {@code hadoop} and {@code s3tables} flavors: they have
 * NO metastore-side CREATE-CATALOG rules (legacy {@code IcebergFileSystemMetaStoreProperties} adds no
 * validation; {@code IcebergS3TablesMetaStoreProperties} only parses {@code S3Properties}, whose checks
 * run upstream at fe-filesystem storage bind — §4 of the P6-T10 design). {@link #validate()} is therefore
 * a no-op; the provider exists only so {@code bindForType("hadoop"/"s3tables")} does not throw.
 */
public final class IcebergNoOpMetaStoreProperties extends AbstractMetaStoreProperties {

    private final String providerName;

    private IcebergNoOpMetaStoreProperties(Map<String, String> raw, String providerName) {
        super(raw);
        this.providerName = providerName;
    }

    public static IcebergNoOpMetaStoreProperties of(Map<String, String> raw, String providerName) {
        IcebergNoOpMetaStoreProperties props = new IcebergNoOpMetaStoreProperties(raw, providerName);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return providerName;
    }

    @Override
    public void validate() {
        // The hadoop flavor restores the legacy IcebergHadoopExternalCatalog constructor's warehouse-required
        // check (a HadoopCatalog cannot initialize without a warehouse root). s3tables shares this class but
        // has no such rule, so the check is gated on the HADOOP provider only. Other storage validation runs
        // upstream at fe-filesystem bind. isEmpty (not isBlank) mirrors the legacy StringUtils.isNotEmpty.
        if ("HADOOP".equals(providerName) && StringUtils.isEmpty(warehouse)) {
            throw new IllegalArgumentException(
                    "Cannot initialize Iceberg HadoopCatalog because 'warehouse' must not be null or empty");
        }
    }
}
