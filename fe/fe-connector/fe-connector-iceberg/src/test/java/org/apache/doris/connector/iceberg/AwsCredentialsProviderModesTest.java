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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * F14: pins {@link AwsCredentialsProviderModes} — the connector's self-contained twin of legacy
 * {@code AwsCredentialsProviderFactory.getV2ClassName / createV2}. Without it, a flipped iceberg catalog with a
 * non-DEFAULT {@code s3.credentials_provider_type} (e.g. ANONYMOUS for a public bucket, or a forced
 * WEB_IDENTITY) silently dropped the pin and fell back to the SDK default chain.
 */
public class AwsCredentialsProviderModesTest {

    private static Map<String, String> mode(String value) {
        Map<String, String> props = new HashMap<>();
        props.put("s3.credentials_provider_type", value);
        return props;
    }

    @Test
    public void classNameForMapsEachNonDefaultModeToItsAwsSdkClass() {
        // MUTATION: dropping/renaming any case, or returning a wrong FQCN, -> the iceberg SDK cannot reflectively
        // load the provider (or loads the wrong one) -> red. Uses .class.getName() (byte-identical to legacy
        // getV2ClassName, which also uses .class.getName()).
        Assertions.assertEquals(EnvironmentVariableCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("ENV"), AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(SystemPropertyCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("SYSTEM_PROPERTIES"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(WebIdentityTokenFileCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("WEB_IDENTITY"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(ContainerCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("CONTAINER"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(InstanceProfileCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("INSTANCE_PROFILE"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(AnonymousCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("ANONYMOUS"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
    }

    @Test
    public void classNameForYieldsNullForDefaultBlankAndAbsent() {
        // DEFAULT / blank / unknown / absent -> null so the caller emits NOTHING (SDK default chain) — mirrors
        // legacy putCredentialsProvider's early DEFAULT return. MUTATION: returning a class for DEFAULT -> the
        // common case wrongly pins DefaultCredentialsProvider by name -> red.
        Assertions.assertNull(AwsCredentialsProviderModes.classNameFor(mode("DEFAULT"),
                AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertNull(AwsCredentialsProviderModes.classNameFor(mode("  "),
                AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertNull(AwsCredentialsProviderModes.classNameFor(mode("bogus"),
                AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertNull(AwsCredentialsProviderModes.classNameFor(Collections.emptyMap(),
                AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertNull(AwsCredentialsProviderModes.classNameFor(null,
                AwsCredentialsProviderModes.S3_MODE_KEYS));
    }

    @Test
    public void resolveModeNormalizesCaseAndHyphenAndPicksFirstNonBlankKey() {
        // Legacy AwsCredentialsProviderMode.fromString normalization: trim / toUpperCase / '-' -> '_'.
        // MUTATION: dropping any normalization step -> "web-identity" / " anonymous " no longer resolve -> red.
        Assertions.assertEquals(WebIdentityTokenFileCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("web-identity"),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        Assertions.assertEquals(AnonymousCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(mode("  anonymous "),
                        AwsCredentialsProviderModes.S3_MODE_KEYS));
        // First non-blank alias wins: blank primary key, value on the second alias.
        Map<String, String> props = new HashMap<>();
        props.put("s3.credentials_provider_type", "");
        props.put("glue.credentials_provider_type", "ENV");
        Assertions.assertEquals(EnvironmentVariableCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(props, AwsCredentialsProviderModes.S3_MODE_KEYS));
        // The iceberg.rest.credentials_provider_type alias MUST be in S3_MODE_KEYS: master binds
        // S3Properties.credentialsProviderType from it, so a glue/s3tables catalog can carry the mode there.
        // MUTATION: dropping that alias from S3_MODE_KEYS -> the pin silently degrades to the default chain -> red.
        Map<String, String> restAlias = new HashMap<>();
        restAlias.put("iceberg.rest.credentials_provider_type", "ANONYMOUS");
        Assertions.assertEquals(AnonymousCredentialsProvider.class.getName(),
                AwsCredentialsProviderModes.classNameFor(restAlias, AwsCredentialsProviderModes.S3_MODE_KEYS));
    }

    @Test
    public void providerForReturnsTheMatchingProviderInstanceAndDefaultsOtherwise() {
        // The s3tables control-plane path needs a live provider instance, not a class name. providerFor is a
        // SECOND switch, independent of classFor, so cover ALL six non-DEFAULT modes. MUTATION: swapping/dropping
        // any case -> the wrong provider (e.g. default where anonymous was requested) -> red.
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("ENV"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof EnvironmentVariableCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("SYSTEM_PROPERTIES"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof SystemPropertyCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("WEB_IDENTITY"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof WebIdentityTokenFileCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("CONTAINER"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof ContainerCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("INSTANCE_PROFILE"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof InstanceProfileCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("ANONYMOUS"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof AnonymousCredentialsProvider);
        // DEFAULT / blank / absent -> the SDK default chain.
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(mode("DEFAULT"),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof DefaultCredentialsProvider);
        Assertions.assertTrue(AwsCredentialsProviderModes.providerFor(Collections.emptyMap(),
                AwsCredentialsProviderModes.S3_MODE_KEYS) instanceof DefaultCredentialsProvider);
    }
}
