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
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds the Lance object-store options for one dataset.
 *
 * <p>Both the FE, which opens the dataset through the Lance Java SDK, and the BE, which opens it
 * through lance-c, consume the map produced here, so neither can reach a dataset by a
 * configuration the other never saw. They still interpret it with their own pinned Lance: the two
 * agree on object_store, but the Java SDK carries a newer OpenDAL than lance-c, so an option only
 * the newer one knows takes effect on the FE and is ignored on the BE.
 *
 * <p>Options vended by a namespace are merged in as they arrive. The Lance Namespace specification
 * describes {@code storage_options} as configuration "passed directly to Lance", so the protocol
 * defines no key vocabulary of its own and a client cannot assume one. Re-encoding those options
 * into a fixed set of names would silently drop everything outside it, including credentials
 * spelled with a different accepted alias and every non-S3 provider's keys.
 */
public final class LanceStorageOptions {
    private static final Logger LOG = LogManager.getLogger(LanceStorageOptions.class);

    private static final String ENDPOINT = "aws_endpoint";
    private static final String VIRTUAL_HOSTED_STYLE = "aws_virtual_hosted_style_request";
    /** Not prefixed: object_store reports this shared client option as canonical under this name. */
    private static final String ALLOW_HTTP = "allow_http";

    /**
     * Doris backend property to Lance object-store option.
     *
     * <p>The spelling emitted for each option is the one object_store reports as canonical, because
     * that is what {@code StorageOptions::with_env_s3} looks for before pulling the same option out
     * of the process environment:
     *
     * <pre>
     * // lance-io/src/object_store/providers/aws.rs
     * if let Ok(config_key) = AmazonS3ConfigKey::from_str(&amp;key.to_ascii_lowercase())
     *     &amp;&amp; !self.0.contains_key(config_key.as_ref())   // "aws_access_key_id"
     * </pre>
     *
     * <p>Any other accepted alias leaves that check unsatisfied, so a stray {@code AWS_ACCESS_KEY_ID}
     * in the FE or BE environment is inserted next to the catalog's value and the two race in
     * {@code as_s3_options()}'s HashMap - the collision this class exists to prevent, resolved
     * independently per option and per process.
     *
     * <p>Lance's OpenDAL backend accepts these spellings too, as serde aliases of its own field
     * names ({@code #[serde(alias = "aws_access_key_id")]}). Emitting the unprefixed name is what
     * breaks there: an environment-injected canonical alias lands beside it and serde rejects the
     * duplicate field.
     */
    private static final Map<String, String> S3_KEYS = new HashMap<>();

    static {
        S3_KEYS.put("AWS_ACCESS_KEY", "aws_access_key_id");
        S3_KEYS.put("AWS_SECRET_KEY", "aws_secret_access_key");
        S3_KEYS.put("AWS_TOKEN", "aws_session_token");
        S3_KEYS.put("AWS_ENDPOINT", ENDPOINT);
        S3_KEYS.put("AWS_REGION", "aws_region");
    }

    /**
     * Every spelling object_store accepts for the options above, mapped to the one this class emits.
     *
     * <p>object_store resolves an alias and its canonical name to one config key and keeps only one
     * of the two values, chosen by hash order. So a namespace vending {@code endpoint_url} while the
     * catalog contributes {@code endpoint} does not override it - the two survive as separate
     * entries, and the FE and the BE can each end up using a different one. Every accepted alias has
     * to be recognized here, or that race simply moves to the spellings this table misses.
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
            // object_store parses this one into a config key of its own that wins over the generic
            // endpoint: `let endpoint = self.s3_endpoint.or(self.endpoint)`. Left alone it would not
            // displace the catalog's endpoint, and allow_http would be derived from the losing one.
            .put("aws_endpoint_url_s3", ENDPOINT)
            .put("region", "aws_region")
            .put("aws_region", "aws_region")
            .put("virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE)
            .put("aws_virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE)
            // OpenDAL's own field name, of which the two spellings above are serde aliases. Leaving
            // it beside one of them makes a duplicate field, and the S3 operator fails to build.
            .put("enable_virtual_host_style", VIRTUAL_HOSTED_STYLE)
            .put("allow_http", ALLOW_HTTP)
            .put("aws_allow_http", ALLOW_HTTP)
            .build();

    /**
     * Aliases that supersede the catalog's value but keep the spelling the namespace used.
     *
     * <p>{@code token} means an S3 session token to object_store's S3 parser but a bearer token to
     * its Azure one, and this class does not know which provider a dataset uses. Renaming it would
     * corrupt the Azure reading, so it is only used to decide which catalog entry it replaces.
     */
    private static final Set<String> AMBIGUOUS_ALIASES = ImmutableSet.of("token");

    /**
     * Options a namespace may not override, because they decide which data is read rather than how
     * it is accessed. Lance protects the same keys in the options it accepts from a namespace.
     */
    private static final Set<String> PROTECTED_KEYS = ImmutableSet.of(
            "bucket", "aws_bucket", "aws_bucket_name", "bucket_name", "root");

    private LanceStorageOptions() {
    }

    /** Converts normalized Doris storage properties to Lance object-store options. */
    public static Map<String, String> toLanceOptions(Map<String, String> backendProperties) {
        Map<String, String> result = new HashMap<>();
        S3_KEYS.forEach((dorisKey, lanceKey) -> putIfNotEmpty(result, lanceKey,
                backendProperties.get(dorisKey)));

        String usePathStyle = backendProperties.get("use_path_style");
        if (usePathStyle != null && !usePathStyle.isEmpty()) {
            result.put(VIRTUAL_HOSTED_STYLE, String.valueOf(!Boolean.parseBoolean(usePathStyle)));
        }
        return withDerivedAllowHttp(result);
    }

    /**
     * Merges the options a namespace vended for one table over the catalog's own options.
     *
     * <p>Options a namespace may not override are dropped; everything else replaces the catalog
     * value, since the namespace decides how the table it just described is reached.
     */
    public static Map<String, String> mergeVended(Map<String, String> lanceOptions,
            Map<String, String> vendedOptions) {
        Map<String, String> result = new HashMap<>(lanceOptions);
        if (vendedOptions == null || vendedOptions.isEmpty()) {
            return result;
        }

        Map<String, String> accepted = new HashMap<>();
        Set<String> superseded = new HashSet<>();
        vendedOptions.forEach((key, value) -> {
            if (key == null || value == null || value.isEmpty()) {
                return;
            }
            if (key.indexOf('\0') >= 0 || value.indexOf('\0') >= 0) {
                // lance-c reads these as C strings, so a NUL would truncate the option there while
                // leaving it unrecognized here - the two halves would disagree about the key.
                LOG.warn("Ignoring Lance storage option vended by the namespace because its key or "
                        + "value contains a NUL character");
                return;
            }
            String lowerCased = key.toLowerCase(Locale.ROOT);
            if (PROTECTED_KEYS.contains(lowerCased)) {
                LOG.warn("Ignoring Lance storage option '{}' vended by the namespace because it "
                        + "would change which data is read", key);
                return;
            }
            String canonical = CANONICAL_BY_ALIAS.get(lowerCased);
            if (canonical != null) {
                superseded.add(canonical);
            }
            accepted.put(canonical != null && !AMBIGUOUS_ALIASES.contains(lowerCased)
                    ? canonical : key, value);
        });

        // Drop the catalog's spelling of every option the namespace just supplied, so the two can
        // never reach Lance as competing entries for one config key.
        result.keySet().removeAll(superseded);
        // allow_http describes the endpoint, so a vended endpoint invalidates a value derived from
        // the catalog's. An explicitly vended allow_http is in `superseded` and survives.
        if (superseded.contains(ENDPOINT) && !superseded.contains(ALLOW_HTTP)) {
            result.remove(ALLOW_HTTP);
        }
        result.putAll(accepted);
        return withDerivedAllowHttp(result);
    }

    /**
     * Allows plain HTTP when the endpoint in use asks for it.
     *
     * <p>Applied after merging, because a namespace can replace the endpoint the catalog was
     * configured with - or supply the only one there is.
     */
    private static Map<String, String> withDerivedAllowHttp(Map<String, String> options) {
        String endpoint = options.get(ENDPOINT);
        if (endpoint != null && endpoint.startsWith("http://")) {
            options.putIfAbsent(ALLOW_HTTP, "true");
        }
        return options;
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
