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

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.iceberg.catalog.Catalog;

import java.util.List;
import java.util.Map;

public class IcebergAliyunDLFMetaStoreProperties extends AbstractIcebergProperties {

    protected IcebergAliyunDLFMetaStoreProperties(Map<String, String> props) {
        super(props);
        super.initNormalizeAndCheckProps();
        // Validate DLF base properties (e.g. endpoint / region) at construction time; the parsed
        // value is no longer retained because native fe-core catalog construction was removed.
        AliyunDLFBaseProperties.of(origProps);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_DLF;
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
                               List<StorageProperties> storagePropertiesList) {
        // Native fe-core DLF Iceberg catalog construction has been removed. After the catalog-SPI
        // flip, DLF Iceberg catalogs are built by the plugin connector
        // (fe-connector-iceberg IcebergConnector#createDlfCatalog); this legacy path is dead and
        // only reachable via the (also dead) native IcebergExternalCatalog. Fail loud if invoked.
        throw new UnsupportedOperationException(
                "Native fe-core DLF Iceberg catalog construction is no longer supported");
    }
}
