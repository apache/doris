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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.Map;

public class IcebergVendedCredentialsProvider {

    // AWS credential property keys for backend
    private static final String BACKEND_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    private static final String BACKEND_AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private static final String BACKEND_AWS_TOKEN = "AWS_TOKEN";

    /**
     * Interface for future FileIO-based credential extraction.
     * This interface is designed to be compatible with Iceberg FileIO
     * when we implement FileIO support in the future.
     */
    public interface CredentialExtractor {
        /**
         * Extract credentials from a generic properties map.
         *
         * @param properties properties map from any source (FileIO, Table IO, etc.)
         * @return extracted credentials as backend properties
         */
        Map<String, String> extractCredentials(Map<String, String> properties);
    }

    /**
     * Default credential extractor for S3 credentials.
     */
    public static class S3CredentialExtractor implements CredentialExtractor {
        @Override
        public Map<String, String> extractCredentials(Map<String, String> properties) {
            Map<String, String> credentials = Maps.newHashMap();

            if (properties == null || properties.isEmpty()) {
                return credentials;
            }

            // Extract AWS credentials from Iceberg S3 FileIO format
            if (properties.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
                credentials.put(BACKEND_AWS_ACCESS_KEY, properties.get(S3FileIOProperties.ACCESS_KEY_ID));
            }
            if (properties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
                credentials.put(BACKEND_AWS_SECRET_KEY, properties.get(S3FileIOProperties.SECRET_ACCESS_KEY));
            }
            if (properties.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
                credentials.put(BACKEND_AWS_TOKEN, properties.get(S3FileIOProperties.SESSION_TOKEN));
            }

            return credentials;
        }
    }

    private static final S3CredentialExtractor s3Extractor = new S3CredentialExtractor();

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
     * Future method for FileIO-based credential extraction.
     * This method signature is designed to be compatible with future FileIO implementations.
     *
     * @param fileIoProperties properties from FileIO (reserved for future use)
     * @param extractor custom credential extractor
     * @return extracted credentials
     */
    @VisibleForTesting
    public static Map<String, String> extractCredentialsFromFileIO(Map<String, String> fileIoProperties,
            CredentialExtractor extractor) {
        return extractor.extractCredentials(fileIoProperties);
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
        return locationProperties;
    }
}
