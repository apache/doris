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

package org.apache.doris.connector.iceberg;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.util.Locale;
import java.util.Map;

/**
 * F14: resolves the user's AWS credential provider mode into either the iceberg-SDK
 * {@code client.credentials-provider} class name (for the S3FileIO / REST-signing property maps) or a live AWS
 * SDK v2 provider instance (for the s3tables control-plane client). The connector cannot import the fe-core
 * {@code AwsCredentialsProviderFactory}, so this is a self-contained twin of its
 * {@code getV2ClassName(mode)} / {@code createV2(mode)} plus the {@code AwsCredentialsProviderMode.fromString}
 * normalization ({@code trim / toUpperCase / '-' -> '_'}).
 *
 * <p>The mode string comes from the original catalog properties under {@code s3.credentials_provider_type} (and
 * its aliases) or {@code iceberg.rest.credentials_provider_type}. {@code DEFAULT} — the common case, and also
 * blank / unknown — yields NO explicit class name ({@code null}) and the SDK default-chain provider, exactly
 * mirroring legacy {@code putCredentialsProvider}'s early return for {@code DEFAULT}. Only the six non-DEFAULT
 * modes (which legacy pinned to a specific provider class) were being silently dropped on the connector path
 * because {@link org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties} exposes no
 * provider-mode accessor. {@code AwsCredentialsProviderModesTest} pins the emitted class names against the AWS
 * SDK classes so a drift fails loud.
 */
final class AwsCredentialsProviderModes {

    // Aliases for the S3 store's credential-provider mode (generic S3 / glue / s3tables signing paths).
    // Byte-identical to the alias set master binds S3Properties.credentialsProviderType from
    // (S3Properties.java @ConnectorProperty: s3.credentials_provider_type / glue.credentials_provider_type /
    // iceberg.rest.credentials_provider_type) — a glue/s3tables PROVIDER_CHAIN catalog may carry the mode under
    // any of the three, so all must be honored or the pin is silently dropped for the rest-alias form.
    static final String[] S3_MODE_KEYS = {
            "s3.credentials_provider_type", "glue.credentials_provider_type",
            "iceberg.rest.credentials_provider_type"};

    private AwsCredentialsProviderModes() {}

    /**
     * The non-DEFAULT provider class name for the first non-blank mode key in {@code props}, or {@code null}
     * for DEFAULT / blank / unknown (so callers emit nothing and the SDK default chain applies).
     */
    static String classNameFor(Map<String, String> props, String... modeKeys) {
        Class<?> clazz = classFor(resolveMode(props, modeKeys));
        return clazz == null ? null : clazz.getName();
    }

    /**
     * The AWS SDK v2 provider instance for the first non-blank mode key in {@code props};
     * {@link DefaultCredentialsProvider} for DEFAULT / blank / unknown.
     */
    static AwsCredentialsProvider providerFor(Map<String, String> props, String... modeKeys) {
        switch (resolveMode(props, modeKeys)) {
            case "ENV":
                return EnvironmentVariableCredentialsProvider.create();
            case "SYSTEM_PROPERTIES":
                return SystemPropertyCredentialsProvider.create();
            case "WEB_IDENTITY":
                return WebIdentityTokenFileCredentialsProvider.create();
            case "CONTAINER":
                return ContainerCredentialsProvider.create();
            case "INSTANCE_PROFILE":
                return InstanceProfileCredentialsProvider.create();
            case "ANONYMOUS":
                return AnonymousCredentialsProvider.create();
            default:
                return DefaultCredentialsProvider.create();
        }
    }

    private static Class<?> classFor(String mode) {
        switch (mode) {
            case "ENV":
                return EnvironmentVariableCredentialsProvider.class;
            case "SYSTEM_PROPERTIES":
                return SystemPropertyCredentialsProvider.class;
            case "WEB_IDENTITY":
                return WebIdentityTokenFileCredentialsProvider.class;
            case "CONTAINER":
                return ContainerCredentialsProvider.class;
            case "INSTANCE_PROFILE":
                return InstanceProfileCredentialsProvider.class;
            case "ANONYMOUS":
                return AnonymousCredentialsProvider.class;
            default:
                // DEFAULT / blank / unknown -> SDK default chain, no explicit class emitted.
                return null;
        }
    }

    /** First non-blank mode value normalized like legacy AwsCredentialsProviderMode.fromString; else "DEFAULT". */
    private static String resolveMode(Map<String, String> props, String... modeKeys) {
        if (props == null) {
            return "DEFAULT";
        }
        for (String key : modeKeys) {
            String value = props.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            }
        }
        return "DEFAULT";
    }
}
