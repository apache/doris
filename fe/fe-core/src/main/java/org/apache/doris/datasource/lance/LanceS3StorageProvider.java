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

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * S3-compatible storage, which Lance reaches through object_store's AWS provider.
 *
 * <p>The emitted spelling is the one object_store reports as canonical, because that is what
 * {@code StorageOptions::with_env_s3} looks for before pulling the same option out of the process
 * environment:
 *
 * <pre>
 * // lance-io/src/object_store/providers/aws.rs
 * if let Ok(config_key) = AmazonS3ConfigKey::from_str(&amp;key.to_ascii_lowercase())
 *     &amp;&amp; !self.0.contains_key(config_key.as_ref())   // "aws_access_key_id"
 * </pre>
 *
 * <p>Any other accepted alias leaves that check unsatisfied, so a stray {@code AWS_ACCESS_KEY_ID}
 * in the FE or BE environment is inserted next to the configured value, and object_store's
 * {@code as_s3_options()} then folds both onto one config key and keeps whichever its HashMap
 * yields last - independently per option and per process. Lance's OpenDAL S3 backend accepts these
 * same spellings as serde aliases of its own field names.
 */
final class LanceS3StorageProvider implements LanceStorageProvider {

    static final LanceS3StorageProvider INSTANCE = new LanceS3StorageProvider();

    private static final String ENDPOINT = "aws_endpoint";
    private static final String VIRTUAL_HOSTED_STYLE = "aws_virtual_hosted_style_request";
    /**
     * Not prefixed: object_store carries this as a shared client option, and reports it as
     * canonical under this name. The spelling buys nothing against the environment here, though -
     * {@code StorageOptions::new} overwrites this key outright from {@code AWS_ALLOW_HTTP} before
     * {@code with_env_s3} runs.
     */
    private static final String ALLOW_HTTP = "allow_http";

    /** Doris backend property to Lance object-store option. */
    private static final Map<String, String> DORIS_KEYS = ImmutableMap.<String, String>builder()
            .put("AWS_ACCESS_KEY", "aws_access_key_id")
            .put("AWS_SECRET_KEY", "aws_secret_access_key")
            .put("AWS_TOKEN", "aws_session_token")
            .put("AWS_ENDPOINT", ENDPOINT)
            .put("AWS_REGION", "aws_region")
            .build();

    /**
     * Every spelling object_store accepts for the options above, mapped onto the one emitted.
     *
     * <p>Confined to those options on purpose. They are the only ones this class contributes, so
     * they are the only ones a vended option can collide with; anything else a namespace sends is
     * between the namespace and Lance.
     *
     * <p>{@code token} is included because this provider only ever speaks for an S3 dataset, where
     * it is unambiguously the session token. It means a bearer token to object_store's Azure
     * parser, which is why it can only be resolved once the provider is known.
     *
     * <p>{@code aws_endpoint_url_s3} is deliberately absent. object_store parses it into a config
     * key of its own and prefers it over the generic endpoint, so a vended one already wins
     * without being rewritten, and folding it in would replace a defined precedence with map order.
     */
    private static final Map<String, String> CANONICAL_BY_ALIAS = ImmutableMap.<String, String>builder()
            .put("access_key_id", "aws_access_key_id")
            .put("aws_access_key_id", "aws_access_key_id")
            .put("secret_access_key", "aws_secret_access_key")
            .put("aws_secret_access_key", "aws_secret_access_key")
            .put("session_token", "aws_session_token")
            .put("aws_session_token", "aws_session_token")
            .put("aws_token", "aws_session_token")
            .put("token", "aws_session_token")
            .put("endpoint", ENDPOINT)
            .put("endpoint_url", ENDPOINT)
            .put("aws_endpoint", ENDPOINT)
            .put("aws_endpoint_url", ENDPOINT)
            .put("region", "aws_region")
            .put("aws_region", "aws_region")
            .put("virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE)
            .put("aws_virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE)
            // OpenDAL's own field name, of which the two above are serde aliases. All three would
            // be the same field supplied more than once, which fails the operator build outright.
            .put("enable_virtual_host_style", VIRTUAL_HOSTED_STYLE)
            .put("allow_http", ALLOW_HTTP)
            .put("aws_allow_http", ALLOW_HTTP)
            .build();

    private LanceS3StorageProvider() {
    }

    @Override
    public Map<String, String> fromDorisProperties(Map<String, String> backendProperties) {
        Map<String, String> result = new HashMap<>();
        if (backendProperties == null) {
            return result;
        }
        DORIS_KEYS.forEach((dorisKey, lanceKey) -> putIfNotEmpty(result, lanceKey,
                backendProperties.get(dorisKey)));

        String usePathStyle = backendProperties.get("use_path_style");
        if (usePathStyle != null && !usePathStyle.isEmpty()) {
            result.put(VIRTUAL_HOSTED_STYLE, String.valueOf(!Boolean.parseBoolean(usePathStyle)));
        }

        // Lance refuses a plain-HTTP endpoint unless this is set, and Doris configures one for
        // MinIO. It describes the endpoint just mapped, so it is derived from the same properties.
        String endpoint = backendProperties.get("AWS_ENDPOINT");
        if (endpoint != null && endpoint.startsWith("http://")) {
            result.put(ALLOW_HTTP, "true");
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
            String canonical = CANONICAL_BY_ALIAS.getOrDefault(key.toLowerCase(Locale.ROOT), key);
            String previous = result.put(canonical, value);
            // Two spellings of one option, disagreeing. Picking one would be the coin toss this
            // whole class exists to remove, so say so instead.
            if (previous != null && !previous.equals(value)) {
                throw new IllegalArgumentException(
                        "Lance namespace vended conflicting values for storage option '"
                                + canonical + "'");
            }
        });
        return result;
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
