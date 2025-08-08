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

import org.apache.doris.datasource.credentials.CredentialExtractor;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;

/**
 * Default credential extractor for S3 credentials.
 */
public class PaimonOssCredentialExtractor implements CredentialExtractor {
    // oss credential property keys from DLF Paimon FileIO
    private static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    private static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_ENDPOINT = "fs.oss.endpoint";

    @Override
    public Map<String, String> extractCredentials(Map<String, String> properties) {
        Map<String, String> credentials = Maps.newHashMap();

        if (properties == null || properties.isEmpty()) {
            return credentials;
        }

        // Extract AWS credentials from Iceberg S3 FileIO format
        if (properties.containsKey(OSS_ACCESS_KEY)) {
            credentials.put(BACKEND_AWS_ACCESS_KEY, properties.get(OSS_ACCESS_KEY));
        }
        if (properties.containsKey(OSS_SECRET_KEY)) {
            credentials.put(BACKEND_AWS_SECRET_KEY, properties.get(OSS_SECRET_KEY));
        }
        if (properties.containsKey(OSS_SECURITY_TOKEN)) {
            credentials.put(BACKEND_AWS_TOKEN, properties.get(OSS_SECURITY_TOKEN));
        }
        if (properties.containsKey(OSS_ENDPOINT)) {
            credentials.put(BACKEND_AWS_ENDPOINT, properties.get(OSS_ENDPOINT));
            Optional<String> regionOpt = AbstractS3CompatibleProperties.extractRegion(
                    OSSProperties.ENDPOINT_PATTERN, properties.get(OSS_ENDPOINT));
            if (regionOpt.isPresent()) {
                credentials.put(BACKEND_AWS_REGION, regionOpt.get());
            }
        }

        return credentials;
    }
}
