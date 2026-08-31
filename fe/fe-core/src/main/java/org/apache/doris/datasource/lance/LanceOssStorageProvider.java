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
        putIfNotEmpty(result, REGION, properties.getRegion());
        putAuth(result, properties.getAccessKey(), properties.getSecretKey(),
                properties.getSessionToken());

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

    /**
     * Chooses which side's OSS authentication to keep once a namespace's options have been laid
     * over the catalog's.
     *
     * <p>Authentication is one value, not a set of independent keys: a key pair, the token that
     * belongs to that pair, and the flag saying there is no pair at all. The overlay that produced
     * {@code merged} is key by key, so on its own it can pair one side's access key with the
     * other's secret, or leave a token attached to a pair it was never issued for. OpenDAL does not
     * second-guess any of that - {@code OssCore::sign} returns the request unsigned whenever
     * {@code allow_anonymous} is set, whatever credentials sit beside it, and it signs with
     * whatever token it finds next to a pair.
     *
     * <p>So the two sides are never mixed. A namespace that supplies any part of the authentication
     * has just described this table and supplies all of it; otherwise the catalog's stands
     * untouched. Every case the merge could previously get wrong - a half pair, a stale token, a
     * vended blank, an explicit anonymous request - follows from that one rule.
     *
     * <p>This lives here rather than on {@link LanceStorageProvider} because OSS is the only
     * provider whose options carry the coupling. The S3 adapter has a comparable overlay but
     * predates this work, and a hook only one implementation honoured would read as an oversight.
     */
    static void reconcileAuth(Map<String, String> merged, Map<String, String> normalizedVended) {
        boolean vendedSuppliesAuth = normalizedVended.containsKey(ACCESS_KEY_ID)
                || normalizedVended.containsKey(SECRET_ACCESS_KEY)
                || normalizedVended.containsKey(SECURITY_TOKEN)
                || normalizedVended.containsKey(ALLOW_ANONYMOUS);
        if (!vendedSuppliesAuth) {
            return;
        }
        putAuth(merged, normalizedVended.get(ACCESS_KEY_ID),
                normalizedVended.get(SECRET_ACCESS_KEY), normalizedVended.get(SECURITY_TOKEN));
    }

    /**
     * Writes one authentication state into {@code options}, replacing whatever was there.
     *
     * <p>States the anonymous flag either way rather than only when true. Absence is not neutral:
     * lance snapshots the host's {@code OSS_}/{@code AWS_} environment into the same config map
     * before these options are applied, so an omitted key hands the decision to an exported
     * {@code OSS_ALLOW_ANONYMOUS}.
     *
     * <p>Blank is not a credential, so a blank pair is anonymous rather than a signer built from
     * empty strings, and a token is only carried when there is a pair for it to belong to.
     */
    private static void putAuth(Map<String, String> options, String keyId, String secret,
            String token) {
        options.remove(ACCESS_KEY_ID);
        options.remove(SECRET_ACCESS_KEY);
        options.remove(SECURITY_TOKEN);

        boolean hasKeyId = StringUtils.isNotEmpty(keyId);
        boolean hasSecret = StringUtils.isNotEmpty(secret);
        if (hasKeyId != hasSecret) {
            throw new IllegalArgumentException("Incomplete OSS credential: '"
                    + (hasKeyId ? SECRET_ACCESS_KEY : ACCESS_KEY_ID) + "' is missing");
        }
        if (hasKeyId) {
            options.put(ACCESS_KEY_ID, keyId);
            options.put(SECRET_ACCESS_KEY, secret);
            putIfNotEmpty(options, SECURITY_TOKEN, token);
        }
        options.put(ALLOW_ANONYMOUS, String.valueOf(!hasKeyId));
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
