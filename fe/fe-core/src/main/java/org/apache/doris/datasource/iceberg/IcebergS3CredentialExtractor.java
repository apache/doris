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

import org.apache.doris.datasource.credentials.CredentialExtractor;

import com.google.common.collect.Maps;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.Map;

/**
 * Default credential extractor for S3 credentials.
 */
public class IcebergS3CredentialExtractor implements CredentialExtractor {

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
