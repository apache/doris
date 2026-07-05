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

package org.apache.doris.connector.metastore.paimon.dlf;

import org.apache.doris.connector.metastore.spi.AbstractDlfMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * Paimon's Aliyun DLF backend. The {@code dlf.catalog.*} conf and the shared AK/SK/endpoint connection
 * rules live in {@link AbstractDlfMetaStoreProperties}; paimon's {@link #validate()} adds the
 * {@code requireWarehouse()} (every paimon flavor) and the paimon-specific {@link #requireOssStorage()}.
 * Fire order (warehouse → ak → sk → endpoint → oss) is byte-identical to the pre-split
 * {@code DlfMetaStorePropertiesImpl}.
 */
public final class PaimonDlfMetaStoreProperties extends AbstractDlfMetaStoreProperties {

    private PaimonDlfMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static PaimonDlfMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        PaimonDlfMetaStoreProperties props = new PaimonDlfMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        requireWarehouse();
        validateConnection();
        // OSS storage is required for a DLF catalog (legacy selected StorageProperties of OSS/OSS_HDFS at
        // init; here we key off the user's OSS prefixes). Outcome-equivalent rejection + same message.
        requireOssStorage();
    }

    private void requireOssStorage() {
        for (String key : raw.keySet()) {
            if (key.startsWith("oss.") || key.startsWith("fs.oss.") || key.startsWith("paimon.fs.oss.")) {
                return;
            }
        }
        // IllegalArgumentException (not legacy's IllegalStateException) to keep validate() uniform with the
        // other fail-fast rules; the message is byte-identical to legacy and the framework wraps both the
        // same way. (toDlfCatalogConf's blank-endpoint guard keeps ISE to match buildDlfHiveConf.)
        throw new IllegalArgumentException("Paimon DLF metastore requires OSS storage properties.");
    }
}
