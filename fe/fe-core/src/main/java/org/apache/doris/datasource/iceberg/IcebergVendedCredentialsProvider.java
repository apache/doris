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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import com.google.common.collect.Maps;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class IcebergVendedCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergVendedCredentialsProvider.class);

    private static final IcebergS3CredentialExtractor s3Extractor = new IcebergS3CredentialExtractor();

    /**
     * Extract vended credentials from Iceberg Table and convert to backend properties.
     *
     * @param table the Iceberg table
     * @return Map of backend properties with credentials
     */
    public static Map<String, String> extractVendedCredentialsFromTable(Table table) {
        if (table == null || table.io() == null) {
            return Maps.newHashMap();
        }

        Map<String, String> ioProperties = table.io().properties();
        return s3Extractor.extractCredentials(ioProperties);
    }

    /**
     * Get backend location properties for Iceberg catalog with optional vended credentials support.
     * This method extracts the duplicate logic from IcebergScanNode and IcebergTableSink.
     *
     * @param storagePropertiesMap Map of storage properties
     * @param icebergTable Optional Iceberg table for vended credentials extraction
     * @return Map of backend location properties
     */
    public static Map<String, String> getBackendLocationProperties(
            MetastoreProperties metastoreProperties,
            Map<Type, StorageProperties> storagePropertiesMap,
            Table icebergTable) {
        boolean vendedCredentialsEnabled = false;
        if (metastoreProperties instanceof IcebergRestProperties) {
            vendedCredentialsEnabled =
                    ((IcebergRestProperties) metastoreProperties).isIcebergRestVendedCredentialsEnabled();
        }
        Map<String, String> locationProperties = Maps.newHashMap();
        for (StorageProperties storageProperties : storagePropertiesMap.values()) {
            if (vendedCredentialsEnabled && icebergTable != null) {
                Map<String, String> vendedCredentials = extractVendedCredentialsFromTable(icebergTable);
                locationProperties.putAll(storageProperties.getBackendConfigProperties(vendedCredentials));
            } else {
                locationProperties.putAll(storageProperties.getBackendConfigProperties());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Iceberg backend location properties: {}, vendedCredentialsEnabled: {}, icebergTable: {}",
                    locationProperties, vendedCredentialsEnabled, icebergTable != null ? icebergTable.name() : "null");
        }
        return locationProperties;
    }
}
