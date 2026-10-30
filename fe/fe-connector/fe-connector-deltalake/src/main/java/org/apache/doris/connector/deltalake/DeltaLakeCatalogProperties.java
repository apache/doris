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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Catalog-owned choices; metastore and storage properties stay with their respective binders. */
final class DeltaLakeCatalogProperties {
    static final String CATALOG_TYPE = "deltalake.catalog.type";
    static final String FILESYSTEM = "filesystem";
    static final String HMS = "hms";

    @ConnectorProperty(names = {CATALOG_TYPE}, required = false,
            description = "Delta metadata source: filesystem (default) or hms.")
    private String catalogType = FILESYSTEM;

    @ConnectorProperty(names = {"enable.mapping.timestamp_tz"}, required = false,
            description = "Map Delta timestamps representing instants to TIMESTAMPTZ.")
    private boolean mapTimestampTz;

    @ConnectorProperty(names = {"enable.mapping.varbinary"}, required = false,
            description = "Map Delta binary columns to VARBINARY instead of STRING.")
    private boolean mapVarbinary;

    // Shared HMS client setting, also consumed by Hive and Hudi.
    @ConnectorProperty(names = {"hive.metastore.client.pool.size"}, required = false,
            description = "Maximum number of pooled Hive Metastore clients.")
    private int hmsClientPoolSize = 8;

    private final Map<String, String> raw;
    private MetaStoreProperties metastore;

    private DeltaLakeCatalogProperties(Map<String, String> properties) {
        raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    static DeltaLakeCatalogProperties of(Map<String, String> properties) {
        DeltaLakeCatalogProperties result = new DeltaLakeCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(result, properties);
        result.catalogType = result.catalogType.toLowerCase(Locale.ROOT);
        if (!FILESYSTEM.equals(result.catalogType) && !HMS.equals(result.catalogType)) {
            throw new IllegalArgumentException("Unsupported " + CATALOG_TYPE + ": " + result.catalogType
                    + "; expected filesystem or hms");
        }
        result.metastore = MetaStoreProviders.bindForType(result.catalogType, properties, Collections.emptyMap());
        result.metastore.validate();
        new ParamRules().check(() -> result.hmsClientPoolSize < 0,
                "hive.metastore.client.pool.size must not be negative").validate();
        return result;
    }

    String getCatalogType() {
        return catalogType;
    }

    MetaStoreProperties getMetastore() {
        return metastore;
    }

    boolean isMapTimestampTz() {
        return mapTimestampTz;
    }

    boolean isMapVarbinary() {
        return mapVarbinary;
    }

    int getHmsClientPoolSize() {
        return hmsClientPoolSize;
    }

    Map<String, String> getRaw() {
        return raw;
    }
}
