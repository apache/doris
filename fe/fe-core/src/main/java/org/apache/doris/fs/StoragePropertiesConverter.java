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

package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts legacy {@link StorageProperties} objects to the {@code Map<String, String>} format
 * expected by {@link org.apache.doris.filesystem.FileSystemProvider#supports} and
 * {@link org.apache.doris.filesystem.FileSystemProvider#create}.
 *
 * <p>This class is the ONLY place in fe-core that knows about the mapping between
 * StorageProperties subtypes and filesystem property keys. All other code in fe-core
 * should use {@link FileSystemFactory#get(Map)} or {@link FileSystemFactory#get(StorageProperties)}.
 */
public final class StoragePropertiesConverter {

    private StoragePropertiesConverter() {}

    /**
     * Converts a {@link StorageProperties} instance to a flat {@code Map<String, String>}
     * suitable for passing to {@code FileSystemProvider.supports()} and
     * {@code FileSystemProvider.create()}.
     *
     * <p>Uses {@code getBackendConfigProperties()} as the base map and injects
     * additional type-marker keys so that providers can unambiguously identify themselves.
     */
    public static Map<String, String> toMap(StorageProperties props) {
        Map<String, String> map = new HashMap<>(props.getBackendConfigProperties());

        if (props instanceof AbstractS3CompatibleProperties) {
            AbstractS3CompatibleProperties s3Props = (AbstractS3CompatibleProperties) props;
            map.put("AWS_ENDPOINT", s3Props.getEndpoint());
            map.put("AWS_ACCESS_KEY", s3Props.getAccessKey());
            map.put("AWS_REGION", s3Props.getRegion());
            map.put("_STORAGE_TYPE_", s3Props.getStorageName());
            // Bucket is required by cloud-specific operations (listObjectsWithPrefix, getPresignedUrl, etc.)
            if (StringUtils.isNotBlank(s3Props.getBucket())) {
                map.put("AWS_BUCKET", s3Props.getBucket());
            }
            // Pass through STS role ARN and external ID when present in original properties
            String roleArn = s3Props.getOrigProps().get("sts.role_arn");
            if (StringUtils.isNotBlank(roleArn)) {
                map.put("AWS_ROLE_ARN", roleArn);
            }
            String externalId = s3Props.getOrigProps().get("sts.external_id");
            if (StringUtils.isNotBlank(externalId)) {
                map.put("AWS_EXTERNAL_ID", externalId);
            }
        } else if (props instanceof AzureProperties) {
            AzureProperties azureProps = (AzureProperties) props;
            map.put("AZURE_ACCOUNT_NAME", azureProps.getAccountName());
            if (StringUtils.isNotBlank(azureProps.getAccountKey())) {
                map.put("AZURE_ACCOUNT_KEY", azureProps.getAccountKey());
            }
            if (StringUtils.isNotBlank(azureProps.getEndpoint())) {
                map.put("AZURE_ENDPOINT", azureProps.getEndpoint());
            }
            map.put("_STORAGE_TYPE_", "AZURE");
        } else if (props instanceof HdfsCompatibleProperties) {
            map.put("_STORAGE_TYPE_", "HDFS");
        } else if (props instanceof BrokerProperties) {
            map.put("_STORAGE_TYPE_", "BROKER");
        }

        return map;
    }
}
