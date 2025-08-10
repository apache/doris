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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;

import java.util.Map;

public class PaimonVendedCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(PaimonVendedCredentialsProvider.class);

    private static final PaimonOssCredentialExtractor ossExtractor = new PaimonOssCredentialExtractor();

    /**
     * Extract vended credentials from Paimon Table and convert to backend properties.
     *
     * @param table the Paimon table
     * @return Map of backend properties with credentials
     */
    public static Map<String, String> extractVendedCredentialsFromTable(String tokenProvider, Table table) {
        if (table == null || table.fileIO() == null) {
            return Maps.newHashMap();
        }

        if (!(table.fileIO() instanceof RESTTokenFileIO)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("File IO of table {} is not RESTTokenFileIO, cannot extract vended credentials: {}",
                        table.name(), table.fileIO().getClass().getName());
            }
            return Maps.newHashMap();
        }

        RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) table.fileIO();
        RESTToken restToken = restTokenFileIO.validToken();
        Map<String, String> tokens = restToken.token();
        if ("dlf".equalsIgnoreCase(tokenProvider)) {
            return ossExtractor.extractCredentials(tokens);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unsupported token provider: {} for table {}, cannot extract vended credentials",
                        tokenProvider, table.name());
            }
            return Maps.newHashMap();
        }
    }

    /**
     * Get backend location properties for Paimon catalog with optional vended credentials support.
     * Provides backend location properties for a Paimon catalog, including support for vended credentials
     *
     * @param storagePropertiesMap Map of storage properties
     * @param paimonTable Optional Paimon table for vended credentials extraction
     * @return Map of backend location properties
     */
    public static Map<String, String> getBackendLocationProperties(
            MetastoreProperties metastoreProperties,
            Map<Type, StorageProperties> storagePropertiesMap,
            Table paimonTable) {
        Map<String, String> vendedCredentials = Maps.newHashMap();
        if (metastoreProperties instanceof PaimonRestMetaStoreProperties) {
            String tokenProvider = ((PaimonRestMetaStoreProperties) metastoreProperties).getTokenProvider();
            vendedCredentials = extractVendedCredentialsFromTable(tokenProvider, paimonTable);
        }

        Map<String, String> locationProperties = Maps.newHashMap();
        for (StorageProperties storageProperties : storagePropertiesMap.values()) {
            locationProperties.putAll(storageProperties.getBackendConfigProperties(vendedCredentials));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted backend location properties: {} for Paimon table: {}",
                    locationProperties, paimonTable != null ? paimonTable.name() : "null");
        }
        return locationProperties;
    }
}
