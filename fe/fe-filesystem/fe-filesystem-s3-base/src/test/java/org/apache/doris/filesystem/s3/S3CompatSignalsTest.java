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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3CompatSignalsTest {

    @Test
    void hasPrefixKey_matchesOnlyNonBlankValuesWithPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("gs.access_key", "ak");
        props.put("minio.endpoint", "   ");

        Assertions.assertTrue(S3CompatSignals.hasPrefixKey(props, "gs."));
        Assertions.assertFalse(S3CompatSignals.hasPrefixKey(props, "minio."));
        Assertions.assertFalse(S3CompatSignals.hasPrefixKey(props, "ozone."));
    }

    @Test
    void hasAwsOnlyCredentialOptions_flagsRoleArnAndNonStaticProviderTypes() {
        Map<String, String> roleArn = Map.of("sts.role_arn", "arn:aws:iam::1:role/x");
        Map<String, String> instanceProfile =
                Map.of("AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE");
        Map<String, String> allowedDefault = Map.of("AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT");
        Map<String, String> allowedAnonymous =
                Map.of("s3.credentials_provider_type", "ANONYMOUS");
        Map<String, String> plainHmac = Map.of("gs.access_key", "ak", "gs.secret_key", "sk");

        Assertions.assertTrue(S3CompatSignals.hasAwsOnlyCredentialOptions(roleArn));
        Assertions.assertTrue(S3CompatSignals.hasAwsOnlyCredentialOptions(instanceProfile));
        Assertions.assertFalse(S3CompatSignals.hasAwsOnlyCredentialOptions(allowedDefault));
        Assertions.assertFalse(S3CompatSignals.hasAwsOnlyCredentialOptions(allowedAnonymous));
        Assertions.assertFalse(S3CompatSignals.hasAwsOnlyCredentialOptions(plainHmac));
    }

    @Test
    void hasAwsOnlyCredentialOptions_coversGlueAndIcebergRestAliases() {
        Map<String, String> glueExternalId = Map.of("glue.external_id", "ext-id");
        Map<String, String> glueProviderType =
                Map.of("glue.credentials_provider_type", "INSTANCE_PROFILE");
        Map<String, String> icebergRestProviderType =
                Map.of("iceberg.rest.credentials_provider_type", "INSTANCE_PROFILE");
        Map<String, String> lowercaseAnonymous =
                Map.of("AWS_CREDENTIALS_PROVIDER_TYPE", "anonymous");

        Assertions.assertTrue(S3CompatSignals.hasAwsOnlyCredentialOptions(glueExternalId));
        Assertions.assertTrue(S3CompatSignals.hasAwsOnlyCredentialOptions(glueProviderType));
        Assertions.assertTrue(S3CompatSignals.hasAwsOnlyCredentialOptions(icebergRestProviderType));
        Assertions.assertFalse(S3CompatSignals.hasAwsOnlyCredentialOptions(lowercaseAnonymous));
    }

    @Test
    void guessIsGcs_matchesGsEndpointAndGoogleapisAliases() {
        Assertions.assertTrue(S3CompatSignals.guessIsGcs(Map.of("gs.endpoint", "https://custom.example.com")));
        Assertions.assertTrue(S3CompatSignals.guessIsGcs(Map.of("endpoint", "https://storage.googleapis.com")));
        Assertions.assertTrue(S3CompatSignals.guessIsGcs(Map.of("s3.endpoint", "https://storage.googleapis.com")));
        Assertions.assertTrue(S3CompatSignals.guessIsGcs(Map.of("AWS_ENDPOINT", "STORAGE.GOOGLEAPIS.COM")));
        Assertions.assertFalse(S3CompatSignals.guessIsGcs(Map.of("gs.access_key", "ak")));
        Assertions.assertFalse(S3CompatSignals.guessIsGcs(Map.of("endpoint", "https://s3.us-east-1.amazonaws.com")));
        Assertions.assertFalse(S3CompatSignals.guessIsGcs(new HashMap<>()));
    }

    @Test
    void guessIsAwsS3_matchesAmazonawsEndpoints() {
        Assertions.assertTrue(S3CompatSignals.guessIsAwsS3(Map.of("s3.endpoint", "https://s3.us-east-1.amazonaws.com")));
        Assertions.assertTrue(S3CompatSignals.guessIsAwsS3(Map.of("glue.endpoint", "https://glue.us-east-1.amazonaws.com")));
        Assertions.assertFalse(S3CompatSignals.guessIsAwsS3(Map.of("endpoint", "http://127.0.0.1:9000")));
        Assertions.assertFalse(S3CompatSignals.guessIsAwsS3(new HashMap<>()));
    }

    @Test
    void guessIsMinio_requiresMinioKeysAndExcludesAwsAndGcs() {
        Assertions.assertTrue(S3CompatSignals.guessIsMinio(Map.of("minio.endpoint", "http://127.0.0.1:9000")));
        Assertions.assertFalse(S3CompatSignals.guessIsMinio(
                Map.of("minio.access_key", "ak", "s3.endpoint", "https://s3.us-east-1.amazonaws.com")));
        Assertions.assertFalse(S3CompatSignals.guessIsMinio(
                Map.of("minio.access_key", "ak", "s3.endpoint", "https://storage.googleapis.com")));
        Assertions.assertFalse(S3CompatSignals.guessIsMinio(Map.of("s3.access_key", "ak")));
    }

    @Test
    void guessIsOzone_isAlwaysFalseBecauseLegacyHasNoGuess() {
        Assertions.assertFalse(S3CompatSignals.guessIsOzone(Map.of("ozone.endpoint", "http://ozone:9878")));
        Assertions.assertFalse(S3CompatSignals.guessIsOzone(new HashMap<>()));
    }

    @Test
    void hasAnyExplicitFsSupport_detectsAnyFsSupportFlag() {
        Assertions.assertTrue(S3CompatSignals.hasAnyExplicitFsSupport(Map.of("fs.s3.support", "TRUE")));
        Assertions.assertTrue(S3CompatSignals.hasAnyExplicitFsSupport(Map.of("fs.hdfs.support", "true")));
        Assertions.assertTrue(S3CompatSignals.hasAnyExplicitFsSupport(Map.of("oss.hdfs.enabled", "true")));
        Assertions.assertFalse(S3CompatSignals.hasAnyExplicitFsSupport(Map.of("fs.s3.support", "false")));
        Assertions.assertFalse(S3CompatSignals.hasAnyExplicitFsSupport(Map.of("s3.endpoint", "http://x")));
    }

    @Test
    void guessAllowed_isDisabledByExplicitDeclarations() {
        Assertions.assertTrue(S3CompatSignals.guessAllowed(Map.of("s3.endpoint", "http://x")));
        Assertions.assertFalse(S3CompatSignals.guessAllowed(Map.of("fs.minio.support", "true")));
        Assertions.assertFalse(S3CompatSignals.guessAllowed(Map.of("provider", "S3")));
        // A dialect provider hint does not disable guessing for the OTHER dialects; it is simply
        // not an S3 request, and each dialect matches its own hint explicitly first.
        Assertions.assertTrue(S3CompatSignals.guessAllowed(Map.of("provider", "GCP")));
    }

    @Test
    void looksLikeDedicatedDialect_coversGcsAndMinioOnly() {
        Assertions.assertTrue(S3CompatSignals.looksLikeDedicatedDialect(
                Map.of("endpoint", "https://storage.googleapis.com")));
        Assertions.assertTrue(S3CompatSignals.looksLikeDedicatedDialect(Map.of("minio.endpoint", "http://x:9000")));
        Assertions.assertFalse(S3CompatSignals.looksLikeDedicatedDialect(Map.of("ozone.endpoint", "http://x:9878")));
        Assertions.assertFalse(S3CompatSignals.looksLikeDedicatedDialect(
                Map.of("s3.endpoint", "https://s3.us-east-1.amazonaws.com")));
    }

    @Test
    void hasDedicatedDialectRequest_acceptsProviderHintsAndFlags() {
        Assertions.assertTrue(S3CompatSignals.hasDedicatedDialectRequest(Map.of("provider", "gcp")));
        Assertions.assertTrue(S3CompatSignals.hasDedicatedDialectRequest(Map.of("provider", "GCS")));
        Assertions.assertTrue(S3CompatSignals.hasDedicatedDialectRequest(Map.of("fs.ozone.support", "true")));
        Assertions.assertFalse(S3CompatSignals.hasDedicatedDialectRequest(Map.of("provider", "S3")));
        Assertions.assertFalse(S3CompatSignals.hasDedicatedDialectRequest(Map.of("fs.cos.support", "true")));
    }
}
