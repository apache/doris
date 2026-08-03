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

package org.apache.doris.connector.metastore.iceberg.hms;

import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * Iceberg's Hive Metastore (HMS) backend. Conf ({@code toHiveConfOverrides}, consumed by the connector's
 * {@code IcebergCatalogFactory.assembleHiveConf} via {@code bindForType("hms")}) and the connection rules
 * live in the shared {@link AbstractHmsMetaStoreProperties}. Iceberg's {@link #validate()} runs ONLY the
 * connection check — iceberg HMS omits the paimon {@code requireWarehouse()} (legacy
 * {@code IcebergHMSMetaStoreProperties} → {@code HMSBaseProperties.of}; §4 of the P6-T10 design).
 */
public final class IcebergHmsMetaStoreProperties extends AbstractHmsMetaStoreProperties {

    private IcebergHmsMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static IcebergHmsMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        IcebergHmsMetaStoreProperties props = new IcebergHmsMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        validateConnection();
    }
}
