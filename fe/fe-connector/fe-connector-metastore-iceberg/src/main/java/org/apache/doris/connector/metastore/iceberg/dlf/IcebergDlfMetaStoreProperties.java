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

package org.apache.doris.connector.metastore.iceberg.dlf;

import org.apache.doris.connector.metastore.spi.AbstractDlfMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * Iceberg's Aliyun DLF backend. Conf ({@code toDlfCatalogConf}, consumed by the connector's
 * {@code IcebergConnector.createDlfCatalog} via {@code bindForType("dlf")}) and the AK/SK/endpoint
 * connection rules live in the shared {@link AbstractDlfMetaStoreProperties}. Iceberg's
 * {@link #validate()} runs ONLY the connection check — iceberg DLF enforces neither warehouse nor OSS
 * storage (legacy {@code IcebergAliyunDLFMetaStoreProperties} → {@code AliyunDLFBaseProperties.of}; §4 of
 * the P6-T10 design), unlike paimon's DLF.
 */
public final class IcebergDlfMetaStoreProperties extends AbstractDlfMetaStoreProperties {

    private IcebergDlfMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static IcebergDlfMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        IcebergDlfMetaStoreProperties props = new IcebergDlfMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        validateConnection();
    }
}
