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

package org.apache.doris.datasource.credentials;

import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractVendedCredentialsProvider {

    private static final Logger LOG = LogManager.getLogger(AbstractVendedCredentialsProvider.class);

    /**
     * Get temporary storage attribute maps containing vendor credentials
     * This is the core method: format conversion via StorageProperties.createAll()
     *
     * @param metastoreProperties Metastore properties
     * @param tableObject Table object (generics, support different data sources)
     * @return Storage attribute mapping containing temporary credentials
     */
    public final <T> Map<StorageProperties.Type, StorageProperties> getStoragePropertiesMapWithVendedCredentials(
            MetastoreProperties metastoreProperties,
            T tableObject) {

        try {
            if (!isVendedCredentialsEnabled(metastoreProperties) || tableObject == null) {
                return null;
            }

            // 1. Extract original vendored credentials from table objects (such as oss.xxx, s3.xxx format)
            Map<String, String> rawVendedCredentials = extractRawVendedCredentials(tableObject);
            if (rawVendedCredentials == null || rawVendedCredentials.isEmpty()) {
                return null;
            }

            // 2. Filter cloud storage properties before format conversion
            Map<String, String> filteredCredentials =
                    CredentialUtils.filterCloudStorageProperties(rawVendedCredentials);
            if (filteredCredentials.isEmpty()) {
                return null;
            }

            // 3. Key steps: Format conversion via StorageProperties.createAll()
            // This avoids writing duplicate transformation logic in the VendedCredentials class
            List<StorageProperties> vendedStorageProperties = StorageProperties.createAll(filteredCredentials);

            // 4. Convert to Map format
            Map<StorageProperties.Type, StorageProperties> vendedPropertiesMap = vendedStorageProperties.stream()
                    .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully applied vended credentials for table: {}", getTableName(tableObject));
            }
            return vendedPropertiesMap;

        } catch (Exception e) {
            LOG.warn("Failed to get vended credentials, returning null", e);
            // Return null on failure, Fallback is handled by Factory
            return null;
        }
    }

    /**
     * Check whether to enable vendor credentials (subclass implementation)
     */
    protected abstract boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties);

    /**
     * Extract original vendored credentials from table objects (subclass implementation)
     * Returns the original format attribute, which is responsible for the conversion by StorageProperties.createAll()
     */
    protected abstract <T> Map<String, String> extractRawVendedCredentials(T tableObject);

    /**
     * Get the table name (used for logs, subclasses can be rewritable）
     */
    protected <T> String getTableName(T tableObject) {
        return tableObject != null ? tableObject.toString() : "null";
    }

}
