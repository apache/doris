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

import org.apache.doris.datasource.iceberg.IcebergVendedCredentialsProvider;
import org.apache.doris.datasource.paimon.PaimonVendedCredentialsProvider;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import java.util.Map;

public class VendedCredentialsFactory {
    /**
     * Core method: Get temporary storage attribute maps containing vendored credentials for Scan/Sink Node
     * This method is called in Scan/Sink Node.doInitialize() to ensure internal consistency
     */
    public static <T> Map<Type, StorageProperties> getStoragePropertiesMapWithVendedCredentials(
            MetastoreProperties metastoreProperties,
            Map<StorageProperties.Type, StorageProperties> baseStoragePropertiesMap,
            T tableObject) {

        AbstractVendedCredentialsProvider provider = getProviderType(metastoreProperties);
        if (provider != null) {
            try {
                Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                        metastoreProperties, tableObject);
                return result != null ? result : baseStoragePropertiesMap;
            } catch (Exception e) {
                return baseStoragePropertiesMap;
            }
        }

        // Fallback to basic properties
        return baseStoragePropertiesMap;
    }

    /**
     * Select the right provider according to the MetastoreProperties type
     */
    private static AbstractVendedCredentialsProvider getProviderType(MetastoreProperties metastoreProperties) {
        if (metastoreProperties == null) {
            return null;
        }

        MetastoreProperties.Type type = metastoreProperties.getType();
        switch (type) {
            case ICEBERG:
                return IcebergVendedCredentialsProvider.getInstance();
            case PAIMON:
                return PaimonVendedCredentialsProvider.getInstance();
            default:
                // Other types do not support vendor credentials
                return null;
        }
    }
}
