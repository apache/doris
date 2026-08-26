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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Alibaba Cloud OSS storage, which Lance reaches through its OpenDAL OSS provider. */
final class LanceOssStorageProvider implements LanceStorageProvider {

    static final LanceOssStorageProvider INSTANCE = new LanceOssStorageProvider();

    private static final String ENDPOINT = "oss_endpoint";
    private static final String ACCESS_KEY_ID = "oss_access_key_id";
    private static final String SECRET_ACCESS_KEY = "oss_secret_access_key";
    private static final String REGION = "oss_region";
    private static final String SECURITY_TOKEN = "oss_security_token";

    /**
     * Lance exposes the {@code oss_*} names as its public storage-option vocabulary and normalizes
     * them to OpenDAL's field names before constructing the operator. Both spellings are accepted,
     * so collapse only these known pairs before merging static and namespace-vended options.
     */
    private static final Map<String, String> PUBLIC_BY_ALIAS = ImmutableMap.<String, String>builder()
            .put("endpoint", ENDPOINT)
            .put(ENDPOINT, ENDPOINT)
            .put("access_key_id", ACCESS_KEY_ID)
            .put(ACCESS_KEY_ID, ACCESS_KEY_ID)
            .put("access_key_secret", SECRET_ACCESS_KEY)
            .put(SECRET_ACCESS_KEY, SECRET_ACCESS_KEY)
            .put("region", REGION)
            .put(REGION, REGION)
            .put("security_token", SECURITY_TOKEN)
            .put(SECURITY_TOKEN, SECURITY_TOKEN)
            .build();

    private LanceOssStorageProvider() {
    }

    @Override
    public Map<String, String> fromDorisProperties(List<StorageProperties> storageProperties) {
        Map<String, String> result = new HashMap<>();
        OSSProperties properties = selectOss(storageProperties);
        if (properties == null) {
            return result;
        }
        putIfNotEmpty(result, ENDPOINT, properties.getEndpoint());
        putIfNotEmpty(result, ACCESS_KEY_ID, properties.getAccessKey());
        putIfNotEmpty(result, SECRET_ACCESS_KEY, properties.getSecretKey());
        putIfNotEmpty(result, REGION, properties.getRegion());
        putIfNotEmpty(result, SECURITY_TOKEN, properties.getSessionToken());
        return result;
    }

    @Override
    public Map<String, String> normalizeVended(Map<String, String> vendedOptions) {
        Map<String, String> result = new HashMap<>();
        if (vendedOptions == null) {
            return result;
        }
        vendedOptions.forEach((key, value) -> {
            String publicKey = PUBLIC_BY_ALIAS.getOrDefault(key, key);
            String previous = result.put(publicKey, value);
            if (previous != null && !previous.equals(value)) {
                throw new IllegalArgumentException(
                        "Lance namespace vended conflicting values for storage option '"
                                + publicKey + "'");
            }
        });
        return result;
    }

    private static OSSProperties selectOss(List<StorageProperties> storageProperties) {
        if (storageProperties == null) {
            return null;
        }
        for (StorageProperties candidate : storageProperties) {
            if (candidate instanceof OSSProperties) {
                return (OSSProperties) candidate;
            }
        }
        return null;
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
