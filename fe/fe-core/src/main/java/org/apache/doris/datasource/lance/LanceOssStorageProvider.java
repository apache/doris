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
import org.apache.commons.lang3.StringUtils;

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
    private static final String ADDRESSING_STYLE = "addressing_style";
    private static final String ALLOW_ANONYMOUS = "allow_anonymous";

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

        // Doris reads a blank key pair as a request for anonymous access. Lance forwards options it
        // does not recognize straight to OpenDAL, whose OSS service only skips request signing when
        // allow_anonymous is set; without it the open fails in credential loading instead of
        // issuing the unsigned request Doris asked for. Decide from what was actually emitted above
        // rather than re-testing the properties, so a credential this class considers present can
        // never be paired with a claim that there is none.
        if (!result.containsKey(ACCESS_KEY_ID) && !result.containsKey(SECRET_ACCESS_KEY)) {
            result.put(ALLOW_ANONYMOUS, "true");
        }

        // Lance snapshots the host's OSS_*/AWS_*/ALIBABA_CLOUD_* environment into the same config
        // map before storage options are applied, so state both addressing styles explicitly the
        // way the S3 provider does. Leaving the default implicit would let an exported
        // OSS_ADDRESSING_STYLE outrank an explicit oss.use_path_style=false.
        String usePathStyle = properties.getUsePathStyle();
        if (StringUtils.isNotEmpty(usePathStyle)) {
            result.put(ADDRESSING_STYLE, Boolean.parseBoolean(usePathStyle) ? "path" : "virtual");
        }
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
