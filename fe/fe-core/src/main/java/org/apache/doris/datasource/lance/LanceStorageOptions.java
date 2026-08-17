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
 * through lance-c, consume the map produced here, so the two cannot disagree about how a dataset
 * is accessed.
 *
 * <p>Options vended by a namespace are merged in as they arrive. The Lance Namespace specification
 * describes {@code storage_options} as configuration "passed directly to Lance", so the protocol
 * defines no key vocabulary of its own and a client cannot assume one. Re-encoding those options
 * into a fixed set of names would silently drop everything outside it, including credentials
 * spelled with a different accepted alias and every non-S3 provider's keys.
 */
public final class LanceStorageOptions {
    private static final Logger LOG = LogManager.getLogger(LanceStorageOptions.class);

    /**
     * Doris backend property to Lance object-store option.
     *
     * <p>Lance reaches S3 through object_store, which accepts both {@code access_key_id} and
     * {@code aws_access_key_id}. The unprefixed spelling is chosen because it is also the field
     * name used by the OpenDAL backend, which performs no alias normalization at all, so these
     * options stay correct if that backend is ever selected.
     */
    private static final Map<String, String> S3_KEYS = new HashMap<>();

    static {
        S3_KEYS.put("AWS_ACCESS_KEY", "access_key_id");
        S3_KEYS.put("AWS_SECRET_KEY", "secret_access_key");
        S3_KEYS.put("AWS_TOKEN", "session_token");
        S3_KEYS.put("AWS_ENDPOINT", "endpoint");
        S3_KEYS.put("AWS_REGION", "region");
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
            .put("access_key_id", "access_key_id")
            .put("aws_access_key_id", "access_key_id")
            .put("secret_access_key", "secret_access_key")
            .put("aws_secret_access_key", "secret_access_key")
            .put("session_token", "session_token")
            .put("aws_session_token", "session_token")
            .put("aws_token", "session_token")
            .put("token", "session_token")
            .put("endpoint", "endpoint")
            .put("endpoint_url", "endpoint")
            .put("aws_endpoint", "endpoint")
            .put("aws_endpoint_url", "endpoint")
            .put("region", "region")
            .put("aws_region", "region")
            .put("virtual_hosted_style_request", "virtual_hosted_style_request")
            .put("aws_virtual_hosted_style_request", "virtual_hosted_style_request")
            .put("allow_http", "allow_http")
            .put("aws_allow_http", "allow_http")
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
            result.put("virtual_hosted_style_request",
                    String.valueOf(!Boolean.parseBoolean(usePathStyle)));
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
        if (superseded.contains("endpoint") && !superseded.contains("allow_http")) {
            result.remove("allow_http");
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
        String endpoint = options.get("endpoint");
        if (endpoint != null && endpoint.startsWith("http://")) {
            options.putIfAbsent("allow_http", "true");
        }
        return options;
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
