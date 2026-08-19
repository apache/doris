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

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the Lance object-store options for one dataset.
 *
 * <p>Both the FE, which opens the dataset through the Lance Java SDK, and the BE, which opens it
 * through lance-c, consume the map produced here, so neither can reach a dataset by a
 * configuration the other never saw.
 *
 * <p>Doris translates only its own static storage configuration. Options a namespace vends for a
 * table are overlaid key for key and otherwise left alone: the Lance Namespace specification
 * describes {@code storage_options} as configuration "passed directly to Lance", so the protocol
 * defines no key vocabulary of its own and a client cannot assume one.
 *
 * <p>In particular, vended keys are never renamed onto a canonical spelling. This class does not
 * know which provider a dataset uses, and the accepted spellings differ per provider: object_store's
 * Azure parser reads {@code endpoint} but not {@code aws_endpoint}, and Lance's OSS provider
 * requires {@code endpoint} and reads {@code access_key_id}. Canonicalizing onto the S3 names would
 * silently destroy a working configuration for every non-S3 backend, so a vended key reaches Lance
 * exactly as the namespace wrote it, and resolving equivalent spellings is left to Lance.
 */
public final class LanceStorageOptions {

    /**
     * Doris backend property to Lance object-store option.
     *
     * <p>Only S3-compatible storage is translated, which is all a Lance catalog's own properties
     * describe today. The spelling is the one object_store reports as canonical, which is what
     * {@code StorageOptions::with_env_s3} looks for before pulling the same option out of the
     * process environment, and which Lance's OpenDAL S3 backend accepts as a serde alias of its
     * own field names.
     */
    private static final Map<String, String> S3_KEYS = new HashMap<>();

    static {
        S3_KEYS.put("AWS_ACCESS_KEY", "aws_access_key_id");
        S3_KEYS.put("AWS_SECRET_KEY", "aws_secret_access_key");
        S3_KEYS.put("AWS_TOKEN", "aws_session_token");
        S3_KEYS.put("AWS_ENDPOINT", "aws_endpoint");
        S3_KEYS.put("AWS_REGION", "aws_region");
    }

    private LanceStorageOptions() {
    }

    /** Converts normalized Doris storage properties to Lance object-store options. */
    public static Map<String, String> toLanceOptions(Map<String, String> backendProperties) {
        Map<String, String> result = new HashMap<>();
        S3_KEYS.forEach((dorisKey, lanceKey) -> putIfNotEmpty(result, lanceKey,
                backendProperties.get(dorisKey)));

        String usePathStyle = backendProperties.get("use_path_style");
        if (usePathStyle != null && !usePathStyle.isEmpty()) {
            result.put("aws_virtual_hosted_style_request",
                    String.valueOf(!Boolean.parseBoolean(usePathStyle)));
        }

        // Lance refuses a plain-HTTP endpoint unless this is set, and Doris configures one for
        // MinIO. It describes the endpoint just mapped, so it is derived from the same properties.
        String endpoint = backendProperties.get("AWS_ENDPOINT");
        if (endpoint != null && endpoint.startsWith("http://")) {
            result.put("allow_http", "true");
        }
        return result;
    }

    /**
     * Overlays the options a namespace vended for one table onto the catalog's own.
     *
     * <p>A vended entry replaces the catalog's entry under the same key, and is otherwise added as
     * it arrives. Nothing is renamed, dropped, or interpreted - see the class comment.
     */
    public static Map<String, String> mergeVended(Map<String, String> lanceOptions,
            Map<String, String> vendedOptions) {
        Map<String, String> result = new HashMap<>(lanceOptions);
        if (vendedOptions == null) {
            return result;
        }
        vendedOptions.forEach((key, value) -> {
            validateVendedOption(key, value);
            result.put(key, value);
        });
        return result;
    }

    /**
     * Rejects what the FE cannot hand to the BE unchanged.
     *
     * <p>These options reach lance-c as C strings, so a NUL would truncate one there while the FE
     * kept reading the whole thing, leaving the two halves opening the dataset with different
     * configuration. That has to fail loudly: dropping the option instead just moves the
     * divergence, since an FE that drops it and a BE that does not disagree in the same way.
     */
    private static void validateVendedOption(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Lance namespace vended a storage option with a null key or value");
        }
        if (key.indexOf('\0') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "Lance namespace vended the storage option '" + key.replace('\0', '?')
                            + "' with a NUL in its key or value, which cannot reach the backend");
        }
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
