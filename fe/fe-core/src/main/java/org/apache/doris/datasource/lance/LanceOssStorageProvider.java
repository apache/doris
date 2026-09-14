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
import java.util.Locale;
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
    private static final String SKIP_SIGNATURE = "skip_signature";
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
            // OpenDAL 0.57 renamed this option to skip_signature, but lance-c 0.1.6 still uses
            // OpenDAL 0.56. Emit the old spelling, which newer OpenDAL keeps as an alias.
            .put(SKIP_SIGNATURE, ALLOW_ANONYMOUS)
            .put(ALLOW_ANONYMOUS, ALLOW_ANONYMOUS)
            .build();

    private LanceOssStorageProvider() {
    }

    @Override
    public Map<String, String> normalizeDorisStorageOptions(
            List<StorageProperties> storageProperties) {
        Map<String, String> result = new HashMap<>();
        OSSProperties properties = selectOss(storageProperties);
        if (properties == null) {
            return result;
        }
        putIfNotEmpty(result, ENDPOINT, properties.getEndpoint());
        putIfNotEmpty(result, REGION, properties.getRegion());
        putIfNotEmpty(result, ACCESS_KEY_ID, properties.getAccessKey());
        putIfNotEmpty(result, SECRET_ACCESS_KEY, properties.getSecretKey());
        putIfNotEmpty(result, SECURITY_TOKEN, properties.getSessionToken());

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
    public Map<String, String> normalizeVendedStorageOptions(
            Map<String, String> vendedOptions) {
        Map<String, String> result = new HashMap<>();
        if (vendedOptions == null) {
            return result;
        }
        vendedOptions.forEach((key, value) -> {
            // Look the alias up in lower case, the way the S3 adapter does. OpenDAL lower-cases
            // every key before deserializing its config, so a vended ACCESS_KEY_ID reaches the
            // store as a credential either way - but left unrecognized here it would not count as
            // signing configuration, and the anonymous flag would be inferred as true beside it.
            // An unrecognized key keeps its original spelling: only the store knows what it means.
            String publicKey = PUBLIC_BY_ALIAS.getOrDefault(key.toLowerCase(Locale.ROOT), key);
            String previous = result.put(publicKey, value);
            if (previous != null && !previous.equals(value)) {
                throw new IllegalArgumentException(
                        "Lance namespace vended conflicting values for storage option '"
                                + publicKey + "'");
            }
        });
        return result;
    }

    @Override
    public Map<String, String> inferStorageOptions(Map<String, String> effectiveOptions) {
        Map<String, String> inferred = new HashMap<>();
        boolean hasSigningConfiguration = hasSigningConfiguration(effectiveOptions);
        String allowAnonymous = effectiveOptions.get(ALLOW_ANONYMOUS);
        if (allowAnonymous != null) {
            Boolean anonymous = parseOpenDalBoolean(allowAnonymous);
            if (anonymous == null) {
                throw new IllegalArgumentException("Unrecognized value for OSS storage option '"
                        + ALLOW_ANONYMOUS + "': '" + allowAnonymous
                        + "'. Expected one of true, on, false, off");
            }
            if (anonymous && hasSigningConfiguration) {
                throw new IllegalArgumentException(
                        "Conflicting OSS authentication: anonymous access is enabled but signing "
                                + "credentials are also configured");
            }
            return inferred;
        }
        inferred.put(ALLOW_ANONYMOUS, String.valueOf(!hasSigningConfiguration));
        return inferred;
    }

    /**
     * Reads a flag the way the store will. OpenDAL deserializes its config with a boolean grammar
     * of its own - {@code true|on} and {@code false|off}, anything else refused - so judging a
     * vended value by Java's rules would let {@code on} through as "not anonymous" while the store
     * reads it as anonymous and stops signing. Null for a value OpenDAL would reject, so it can be
     * refused here rather than deep inside the operator build.
     *
     * <p>See {@code opendal-core/src/raw/serde_util.rs}, {@code Pair::deserialize_bool}.
     */
    private static Boolean parseOpenDalBoolean(String value) {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "true":
            case "on":
                return Boolean.TRUE;
            case "false":
            case "off":
                return Boolean.FALSE;
            default:
                return null;
        }
    }

    private static boolean hasSigningConfiguration(Map<String, String> options) {
        return StringUtils.isNotEmpty(options.get(ACCESS_KEY_ID));
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
