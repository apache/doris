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

package org.apache.doris.filesystem.auth;


import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves GCS authentication without creating clients or loading credentials.
 * Vault ALTER still preserves field presence when constructing its patch from this result.
 */
public final class GcsAuthResolver {
    private static final String CREDENTIAL_PROVIDER_TYPE = GcpCredential.CREDENTIAL_PROVIDER_TYPE;
    private static final String IMPERSONATION_SERVICE_ACCOUNT = GcpCredential.IMPERSONATION_SERVICE_ACCOUNT;
    private static final String[] ACCESS_KEYS = {"gs.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key"};
    private static final String[] SECRET_KEYS = {"gs.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key"};
    private static final String[] TOKENS = {"gs.session_token", "s3.session_token", "AWS_TOKEN", "session_token"};
    private static final String[] AWS_PROVIDERS = {"s3.credentials_provider_type", "AWS_CREDENTIALS_PROVIDER_TYPE"};
    private static final String[] AWS_ROLES = {"s3.role_arn", "AWS_ROLE_ARN", "s3.external_id", "AWS_EXTERNAL_ID"};

    private GcsAuthResolver() {
    }

    public static Optional<GcsAuth> resolve(Map<String, String> properties) {
        boolean hasNativeProperties = hasNativeCredentialProperties(properties);
        boolean hasGcsSelector = guessIsGcs(properties)
                || "true".equalsIgnoreCase(getPropertyIgnoreCase(properties, "fs.gcs.support"));
        boolean hasAccessKey = hasNonBlankProperty(properties, ACCESS_KEYS);
        boolean hasSecretKey = hasNonBlankProperty(properties, SECRET_KEYS);
        boolean hasLegacyAnonymous = hasLegacyAnonymousProvider(properties);
        String provider = getPropertyIgnoreCase(properties, "provider");
        boolean useLegacyAnonymousDefault = "S3".equalsIgnoreCase(provider)
                && !hasNonBlankProperty(properties, AWS_PROVIDERS);
        if (isNotBlank(provider) && !"GCP".equalsIgnoreCase(provider) && !"GCS".equalsIgnoreCase(provider)) {
            if (hasNativeProperties) {
                throw new IllegalArgumentException("Native GCP authentication requires provider=GCP, but found: "
                        + provider);
            }
            // Legacy GCS configurations may explicitly select the S3-compatible protocol.
            // Preserve their HMAC/anonymous authentication without enabling native ADC for S3.
            if (!"S3".equalsIgnoreCase(provider) || !hasGcsSelector
                    || !(hasAccessKey || hasSecretKey || hasLegacyAnonymous || useLegacyAnonymousDefault)) {
                return Optional.empty();
            }
        }
        if (!hasNativeProperties && !hasGcsSelector) {
            return Optional.empty();
        }

        String providerType = getPropertyIgnoreCase(properties, CREDENTIAL_PROVIDER_TYPE);
        String impersonation = getPropertyIgnoreCase(properties, IMPERSONATION_SERVICE_ACCOUNT);
        GcsAuth.Mode mode;
        if (hasNativeProperties) {
            GcpCredentialProviderType type = providerType == null ? GcpCredentialProviderType.DEFAULT
                    : GcpCredentialProviderType.fromString(CREDENTIAL_PROVIDER_TYPE, providerType);
            mode = type == GcpCredentialProviderType.ANONYMOUS ? GcsAuth.Mode.ANONYMOUS
                    : type == GcpCredentialProviderType.COMPUTE_ENGINE ? GcsAuth.Mode.COMPUTE_ENGINE : GcsAuth.Mode.ADC;
        } else if (hasLegacyAnonymous) {
            mode = GcsAuth.Mode.ANONYMOUS;
        } else if (hasAccessKey || hasSecretKey) {
            mode = GcsAuth.Mode.HMAC;
        } else if (useLegacyAnonymousDefault) {
            mode = GcsAuth.Mode.ANONYMOUS;
        } else {
            mode = GcsAuth.Mode.ADC;
        }

        // The storage properties validate paired HMAC keys. Raw Vault patches may update only one key.
        if (mode != GcsAuth.Mode.HMAC) {
            if (hasAccessKey || hasSecretKey || hasNonBlankProperty(properties, TOKENS)) {
                throw new IllegalArgumentException(CREDENTIAL_PROVIDER_TYPE
                        + " cannot be used together with access key, secret key, or session token.");
            }
            if (hasNonBlankProperty(properties, AWS_ROLES)
                    || ((hasNativeProperties || mode != GcsAuth.Mode.ANONYMOUS)
                            && hasNonBlankProperty(properties, AWS_PROVIDERS))) {
                throw new IllegalArgumentException("Native GCP authentication cannot be used together with "
                        + "AWS role, external ID, or credentials provider properties.");
            }
        }
        if (mode == GcsAuth.Mode.ANONYMOUS && impersonation != null && !impersonation.isEmpty()) {
            throw new IllegalArgumentException("ANONYMOUS cannot be used with " + IMPERSONATION_SERVICE_ACCOUNT);
        }
        return Optional.of(new GcsAuth(mode, impersonation));
    }

    public static boolean hasNativeCredentialProperties(Map<String, String> properties) {
        return properties.keySet().stream().anyMatch(key -> CREDENTIAL_PROVIDER_TYPE.equalsIgnoreCase(key)
                || IMPERSONATION_SERVICE_ACCOUNT.equalsIgnoreCase(key));
    }

    // Storage selection also uses these signals, before authentication is resolved.
    public static boolean guessIsGcs(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            String value = entry.getValue();
            if ("provider".equals(key) && ("GCP".equalsIgnoreCase(value) || "GCS".equalsIgnoreCase(value))) {
                return true;
            }
            if ((CREDENTIAL_PROVIDER_TYPE.equals(key) || IMPERSONATION_SERVICE_ACCOUNT.equals(key))
                    && isNotBlank(value)) {
                return true;
            }
            if (("uri".equals(key) || "warehouse".equals(key))
                    && value != null && value.regionMatches(true, 0, "gs://", 0, 5)) {
                return true;
            }
            if ("gs.endpoint".equals(key) && isNotBlank(value)) {
                return true;
            }
            if (("s3.endpoint".equals(key) || "aws_endpoint".equals(key) || "endpoint".equals(key))
                    && value != null && value.toLowerCase(Locale.ROOT).endsWith("storage.googleapis.com")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String trim(String value) {
        return value == null ? null : value.trim();
    }

    private static boolean hasLegacyAnonymousProvider(Map<String, String> properties) {
        for (String name : AWS_PROVIDERS) {
            if ("ANONYMOUS".equalsIgnoreCase(trim(getPropertyIgnoreCase(properties, name)))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNonBlankProperty(Map<String, String> properties, String... names) {
        for (String name : names) {
            if (isNotBlank(getPropertyIgnoreCase(properties, name))) {
                return true;
            }
        }
        return false;
    }

    private static String getPropertyIgnoreCase(Map<String, String> properties, String name) {
        return properties.entrySet().stream()
                .filter(entry -> name.equalsIgnoreCase(entry.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
