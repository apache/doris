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

package org.apache.doris.filesystem.s3;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Shared routing/validation signals for the S3-compatible dialects (GCS, MinIO, Ozone).
 *
 * <p>This is dialect plumbing rather than user-facing API, but it must be {@code public}: the
 * dialect providers live in sibling plugin jars that share this package name (a split package,
 * as with {@code fe-filesystem-hdfs-base}). Package-private access across jars only works when
 * both are loaded by the same classloader; the plugin classloaders put them in different runtime
 * packages, so package-private members would fail with {@code IllegalAccessError} at runtime.
 */
public final class S3CompatSignals {

    /** Explicit provider hint, e.g. {@code provider=GCS}. Mirrors StorageProperties.FS_PROVIDER_KEY. */
    public static final String PROVIDER_KEY = "provider";

    public static final String FS_S3_SUPPORT = "fs.s3.support";

    private static final String FS_SUPPORT_PREFIX = "fs.";
    private static final String FS_SUPPORT_SUFFIX = ".support";

    /** Legacy alias for {@code fs.oss-hdfs.support}, honored by StorageProperties.hasAnyExplicitFsSupport. */
    private static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";

    private static final String GCS_ENDPOINT_KEY = "gs.endpoint";
    private static final String GCS_ENDPOINT_SUFFIX = "storage.googleapis.com";
    private static final String AWS_ENDPOINT_INFIX = "amazonaws.com";

    /**
     * Endpoint aliases consulted by the GCS/AWS guesses, compared case-insensitively on the KEY.
     * Union of legacy {@code GCSProperties.GS_ENDPOINT_ALIAS} ({@code s3.endpoint}, {@code AWS_ENDPOINT},
     * {@code endpoint}, {@code ENDPOINT}) and this module's own endpoint aliases.
     */
    private static final List<String> ENDPOINT_ALIASES_LOWER = Collections.unmodifiableList(
            Arrays.asList("gs.endpoint", "s3.endpoint", "aws_endpoint", "endpoint", "aws.endpoint",
                    "glue.endpoint", "aws.glue.endpoint"));

    /** Credential options that only make sense on AWS S3 proper. */
    private static final String[] AWS_ONLY_CREDENTIAL_KEYS = {
            "sts.role_arn", "AWS_ROLE_ARN", S3FileSystemProperties.ROLE_ARN, "glue.role_arn",
            "sts.external_id", "AWS_EXTERNAL_ID", S3FileSystemProperties.EXTERNAL_ID, "glue.external_id"};

    private static final String[] CREDENTIALS_PROVIDER_TYPE_KEYS = {
            S3FileSystemProperties.CREDENTIALS_PROVIDER_TYPE, "AWS_CREDENTIALS_PROVIDER_TYPE",
            "glue.credentials_provider_type", "iceberg.rest.credentials_provider_type"};

    private S3CompatSignals() {}

    /** True when any key with the given prefix carries a non-blank value. */
    public static boolean hasPrefixKey(Map<String, String> properties, String prefix) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null && entry.getKey().startsWith(prefix)
                    && StringUtils.isNotBlank(entry.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the raw map requests an AWS-only credential mechanism (AssumeRole role ARN,
     * external ID, or a credentials provider type other than DEFAULT/ANONYMOUS). GCS/MinIO/Ozone
     * only support static HMAC keys, so their validate() rejects these at binding time instead of
     * failing obscurely at runtime.
     */
    public static boolean hasAwsOnlyCredentialOptions(Map<String, String> rawProperties) {
        for (String key : AWS_ONLY_CREDENTIAL_KEYS) {
            if (StringUtils.isNotBlank(rawProperties.get(key))) {
                return true;
            }
        }
        for (String key : CREDENTIALS_PROVIDER_TYPE_KEYS) {
            String value = rawProperties.get(key);
            if (StringUtils.isNotBlank(value)
                    && !"DEFAULT".equalsIgnoreCase(value.trim())
                    && !"ANONYMOUS".equalsIgnoreCase(value.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The credential provider type the user explicitly requested (trimmed, upper-cased), or
     * {@code null} when none is set. {@link #hasAwsOnlyCredentialOptions} has already restricted any
     * present value to {@code DEFAULT}/{@code ANONYMOUS}, so the result can be handed straight to the
     * shared S3 client; this lets a dialect preserve an explicit {@code DEFAULT} instead of silently
     * downgrading a credential-less map to {@code ANONYMOUS}.
     */
    public static String requestedCredentialsProviderType(Map<String, String> properties) {
        for (String key : CREDENTIALS_PROVIDER_TYPE_KEYS) {
            String value = properties.get(key);
            if (StringUtils.isNotBlank(value)) {
                return value.trim().toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    /** True when {@code key=true} (case-insensitive). Mirrors StorageProperties.isFsSupport. */
    public static boolean isFsSupport(Map<String, String> properties, String key) {
        return "true".equalsIgnoreCase(properties.getOrDefault(key, "false"));
    }

    /**
     * True when the user explicitly declared ANY filesystem via {@code fs.<x>.support=true}.
     *
     * <p>Counterpart of {@code StorageProperties.hasAnyExplicitFsSupport}. That method enumerates the
     * known flags one by one; here the check is structural ({@code fs.*.support}) so that this module
     * never has to know the full dialect list. Same contract: once any explicit flag is present, all
     * {@code guessIsX} heuristics must be disabled so an explicit declaration is never overridden.
     */
    public static boolean hasAnyExplicitFsSupport(Map<String, String> properties) {
        if (isFsSupport(properties, DEPRECATED_OSS_HDFS_SUPPORT)) {
            return true;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith(FS_SUPPORT_PREFIX) && key.endsWith(FS_SUPPORT_SUFFIX)
                    && "true".equalsIgnoreCase(entry.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the user explicitly asked for the generic S3 provider ({@code provider=S3} or
     * {@code fs.s3.support=true}). Dialect providers must not guess against such a map, and the S3
     * provider uses it as its escape hatch from the dialect yield rule.
     *
     * <p>Note the {@code _STORAGE_TYPE_} marker is intentionally NOT consulted: the converter stamps
     * {@code "S3"} on every S3-compatible map, including GCS/MinIO/Ozone ones.
     */
    public static boolean hasExplicitS3Request(Map<String, String> properties) {
        return "S3".equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || isFsSupport(properties, FS_S3_SUPPORT);
    }

    /**
     * True when {@code guessIsX} heuristics may run at all: no explicit {@code fs.*.support=true} and
     * no explicit {@code provider=S3}. Mirrors the {@code useGuess} gate in
     * {@code StorageProperties.createAll/create}.
     */
    public static boolean guessAllowed(Map<String, String> properties) {
        return !hasAnyExplicitFsSupport(properties) && !hasExplicitS3Request(properties);
    }

    /**
     * Port of {@code GCSProperties.guessIsMe}: a non-blank {@code gs.endpoint}, or any endpoint alias
     * whose value ends with {@code storage.googleapis.com}.
     *
     * <p>Deviation: legacy compares {@code key.toLowerCase()} against a set holding the un-lowercased
     * {@code "AWS_ENDPOINT"}/{@code "ENDPOINT"}, so those two aliases can never match there. Here the
     * key comparison is genuinely case-insensitive, which is what legacy clearly intended.
     */
    public static boolean guessIsGcs(Map<String, String> properties) {
        if (StringUtils.isNotBlank(properties.get(GCS_ENDPOINT_KEY))) {
            return true;
        }
        String endpoint = endpointForGuessing(properties);
        return endpoint != null && endpoint.toLowerCase(Locale.ROOT).endsWith(GCS_ENDPOINT_SUFFIX);
    }

    /**
     * Port of {@code S3Properties.guessIsMe}: an endpoint alias whose value contains
     * {@code amazonaws.com}.
     *
     * <p>Deviation: legacy additionally falls back to the {@code uri} and region properties. Those
     * fallbacks are omitted because this predicate is only used here as a mutual-exclusion guard for
     * the dialect guesses (as in {@code MinioProperties.guessIsMe}), never to positively select S3 —
     * the S3 provider claims what no dialect claims.
     */
    public static boolean guessIsAwsS3(Map<String, String> properties) {
        String endpoint = endpointForGuessing(properties);
        return endpoint != null && endpoint.toLowerCase(Locale.ROOT).contains(AWS_ENDPOINT_INFIX);
    }

    /**
     * Port of {@code MinioProperties.guessIsMe}: a non-blank MinIO identifier key, and not AWS S3 nor
     * GCS (legacy also excludes Azure/COS/OSS, which are not visible from this module).
     *
     * <p>Deviation: legacy's {@code IDENTIFIERS} set also contains the generic aliases
     * ({@code AWS_ACCESS_KEY}, {@code endpoint}, {@code s3.access_key}, ...), which only stays safe
     * there because of the Azure/COS/OSS exclusions it can perform. Restricted here to the
     * {@code minio.*} keys so that MinIO cannot silently claim every generic S3-compatible map.
     */
    public static boolean guessIsMinio(Map<String, String> properties) {
        return hasPrefixKey(properties, "minio.")
                && !guessIsAwsS3(properties)
                && !guessIsGcs(properties);
    }

    /**
     * Always {@code false}: legacy {@code OzoneProperties} has NO {@code guessIsMe}, so Ozone is only
     * ever selected through an explicit {@code fs.ozone.support=true} / {@code provider=OZONE}. Kept
     * as a method so the dialect providers share one shape and so the asymmetry is documented rather
     * than invented away with an {@code ozone.*} prefix guess.
     */
    public static boolean guessIsOzone(Map<String, String> properties) {
        return false;
    }

    /**
     * True when the map looks like one of the dedicated dialects hosted here. Used by
     * {@link S3FileSystemProvider} to yield instead of swallowing the map through its generic
     * credential+location fallback.
     */
    public static boolean looksLikeDedicatedDialect(Map<String, String> properties) {
        return guessIsGcs(properties) || guessIsMinio(properties) || guessIsOzone(properties);
    }

    /**
     * True when the map explicitly names one of the dedicated dialects hosted here, via
     * {@code provider=} or {@code fs.<x>.support=true}. {@code GCP} is accepted alongside {@code GCS}
     * because legacy {@code GCSProperties.getBackendConfigProperties} stamps {@code provider=GCP}.
     */
    public static boolean hasDedicatedDialectRequest(Map<String, String> properties) {
        String provider = properties.get(PROVIDER_KEY);
        return "GCS".equalsIgnoreCase(provider)
                || "GCP".equalsIgnoreCase(provider)
                || "MINIO".equalsIgnoreCase(provider)
                || "OZONE".equalsIgnoreCase(provider)
                || isFsSupport(properties, "fs.gcs.support")
                || isFsSupport(properties, "fs.minio.support")
                || isFsSupport(properties, "fs.ozone.support");
    }

    /**
     * First non-blank endpoint value, in declared alias order (so the result does not depend on map
     * iteration order), matching the property key case-insensitively.
     */
    private static String endpointForGuessing(Map<String, String> properties) {
        for (String alias : ENDPOINT_ALIASES_LOWER) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                if (key != null && alias.equals(key.toLowerCase(Locale.ROOT))
                        && StringUtils.isNotBlank(entry.getValue())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }
}
