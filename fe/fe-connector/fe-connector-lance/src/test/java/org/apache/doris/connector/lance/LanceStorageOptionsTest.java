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

package org.apache.doris.connector.lance;

import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** How the catalog's storage keys become the options both FE and BE open a dataset with. */
class LanceStorageOptionsTest {

    private static final String S3_URI = "s3://warehouse/lance/all_types.lance";
    private static final String OSS_URI = "oss://warehouse/table.lance";

    private static FakeS3CompatibleStorageProperties minio() {
        return new FakeS3CompatibleStorageProperties("S3").endpoint("http://minio:9000").region("us-east-1")
                .accessKey("ak").secretKey("sk").usePathStyle("true");
    }

    /** The engine's OSS binding of oss.endpoint / oss.region / oss.access_key / oss.secret_key; path style is false. */
    private static FakeS3CompatibleStorageProperties oss() {
        return new FakeS3CompatibleStorageProperties("OSS").endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .region("cn-hangzhou").accessKey("oss-ak").secretKey("oss-sk").usePathStyle("false");
    }

    /** The same binding without a key pair: anonymous access. */
    private static FakeS3CompatibleStorageProperties anonymousOss() {
        return new FakeS3CompatibleStorageProperties("OSS").endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .region("cn-hangzhou").usePathStyle("false");
    }

    private static List<StorageProperties> catalog(StorageProperties binding) {
        return Collections.singletonList(binding);
    }

    private static Map<String, String> map(String... keysAndValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static String conflict(String uri, List<StorageProperties> storage, Map<String, String> vended) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisAndVendedStorageOptions(uri, storage, vended)).getMessage();
    }

    @Test
    void catalogPropertiesMapToTheCanonicalS3Spelling() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(minio().sessionToken("token")));
        Assertions.assertEquals("ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("token", options.get("aws_session_token"));
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint_url_s3"));
        Assertions.assertEquals("us-east-1", options.get("aws_region"));
        Assertions.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
        // Lance refuses a plain-HTTP endpoint without this.
        Assertions.assertEquals("true", options.get("allow_http"));
        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("aws_access_key_id", "aws_secret_access_key",
                        "aws_session_token", "aws_endpoint", "aws_endpoint_url_s3", "aws_region",
                        "aws_virtual_hosted_style_request", "allow_http")),
                new TreeSet<>(options.keySet()));
    }

    @Test
    void credentiallessAccessFailsInsteadOfSelectingTheNativeCredentialChain() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                        Collections.singletonList(new FakeS3CompatibleStorageProperties("S3")
                                .endpoint("https://s3.amazonaws.com").region("us-east-1"))));
        Assertions.assertTrue(error.getMessage().contains("anonymous and credential-chain access are not supported"));
    }

    @Test
    void vendedS3CredentialsCompleteAnEndpointOnlyBinding() {
        List<StorageProperties> storage = catalog(new FakeS3CompatibleStorageProperties("S3")
                .endpoint("http://minio:9000").region("us-east-1").usePathStyle("true"));
        for (Map<String, String> credentials : Arrays.asList(
                map("access_key_id", "vended-ak", "secret_access_key", "vended-sk"),
                map("AWS_ACCESS_KEY_ID", "vended-ak", "AWS_SECRET_ACCESS_KEY", "vended-sk"))) {
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                    S3_URI, storage, credentials);
            Assertions.assertEquals("vended-ak", options.get("aws_access_key_id"));
            Assertions.assertEquals("vended-sk", options.get("aws_secret_access_key"));
            Assertions.assertEquals("", options.get("aws_session_token"));
            Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint_url_s3"));
        }
        Assertions.assertTrue(conflict(S3_URI, storage, map("access_key_id", "vended-ak"))
                .contains("anonymous and credential-chain access are not supported"));
        Assertions.assertTrue(conflict(S3_URI, catalog(minio()), map("secret_access_key", ""))
                .contains("anonymous and credential-chain access are not supported"));
    }

    @Test
    void vendedS3KeyPairDoesNotInheritAnotherIdentitysSessionToken() {
        Map<String, String> credentials = map("access_key_id", "vended-ak", "secret_access_key", "vended-sk");
        for (List<StorageProperties> storage : Arrays.asList(Collections.<StorageProperties>emptyList(),
                catalog(minio().sessionToken("catalog-session-token")))) {
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                    S3_URI, storage, credentials);
            Assertions.assertEquals("vended-ak", options.get("aws_access_key_id"));
            Assertions.assertEquals("vended-sk", options.get("aws_secret_access_key"));
            Assertions.assertEquals("", options.get("aws_session_token"));
        }
    }

    @Test
    void permanentKeysBlockAmbientSessionTokens() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(minio()));
        Assertions.assertEquals("", options.get("aws_session_token"));
    }

    @Test
    void assumeRoleWithSourceKeysIsRejectedInsteadOfUsingTheSourceIdentity() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                        Collections.singletonList(minio().roleArn("arn:aws:iam::123456789012:role/lance")
                                .externalId("lance-external-id"))));
        Assertions.assertTrue(error.getMessage().contains("AssumeRole access is not supported"));
    }

    @Test
    void finalEndpointBlocksAmbientServiceEndpointsEvenWithExplicitAllowHttp() {
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()),
                Map.of("endpoint", "https://vended.example", "allow_http", "false"));
        Assertions.assertEquals("https://vended.example", options.get("aws_endpoint"));
        Assertions.assertEquals("https://vended.example", options.get("aws_endpoint_url_s3"));
        Assertions.assertEquals("false", options.get("allow_http"));
    }

    @Test
    void vendedServiceEndpointWinsAndControlsHttpInference() {
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio().endpoint("https://catalog.example")),
                Map.of("aws_endpoint_url_s3", "http://vended.example:9000"));
        Assertions.assertEquals("http://vended.example:9000", options.get("aws_endpoint_url_s3"));
        Assertions.assertEquals("true", options.get("allow_http"));

        options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()),
                Map.of("AWS_ENDPOINT_URL_S3", "https://vended.example"));
        Assertions.assertEquals("https://vended.example", options.get("aws_endpoint_url_s3"));
        Assertions.assertFalse(options.containsKey("allow_http"));
    }

    @Test
    void virtualHostedEndpointsContainTheDatasetBucket() {
        for (String endpoint : Arrays.asList("https://s3.us-east-1.amazonaws.com", "s3.us-east-1.amazonaws.com")) {
            Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                    Collections.singletonList(minio().endpoint(endpoint).usePathStyle("false")));
            Assertions.assertEquals("https://warehouse.s3.us-east-1.amazonaws.com", options.get("aws_endpoint"));
            Assertions.assertEquals("true", options.get("aws_virtual_hosted_style_request"));
            Assertions.assertNull(options.get("allow_http"));
        }
        Map<String, String> custom = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(minio().endpoint("http://storage.example:9000/prefix/")
                        .usePathStyle("false")));
        Assertions.assertEquals("http://warehouse.storage.example:9000/prefix/", custom.get("aws_endpoint"));
        Assertions.assertEquals("true", custom.get("allow_http"));
    }

    @Test
    void pathStyleEndpointsDefaultToHttpsAndKeepTheirServiceHost() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(minio().endpoint("s3.us-east-1.amazonaws.com")));
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com", options.get("aws_endpoint"));
        Assertions.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
    }

    @Test
    void inheritedS3EndpointsFollowTheVendedAddressingMode() {
        for (boolean catalogPathStyle : new boolean[] {true, false}) {
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                    catalog(minio().usePathStyle(String.valueOf(catalogPathStyle))),
                    map("virtual_hosted_style_request", String.valueOf(catalogPathStyle)));
            String expected = catalogPathStyle ? "http://warehouse.minio:9000" : "http://minio:9000";
            Assertions.assertEquals(expected, options.get("aws_endpoint"));
            Assertions.assertEquals(expected, options.get("aws_endpoint_url_s3"));
        }
    }

    @Test
    void inheritedS3EndpointsUseTheNativeBooleanGrammar() {
        for (boolean virtualHostedStyle : new boolean[] {true, false}) {
            String[] values = virtualHostedStyle
                    ? new String[] {"1", "true", "on", "yes", "y", "TRUE", "On", "YeS", "Y"}
                    : new String[] {"0", "false", "off", "no", "n", "FALSE", "Off", "nO", "N"};
            for (String value : values) {
                Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                        catalog(minio().usePathStyle(String.valueOf(virtualHostedStyle))),
                        map("virtual_hosted_style_request", value));
                String expected = virtualHostedStyle ? "http://warehouse.minio:9000" : "http://minio:9000";
                Assertions.assertEquals(expected, options.get("aws_endpoint"), value);
                Assertions.assertEquals(expected, options.get("aws_endpoint_url_s3"), value);
                Assertions.assertEquals(value, options.get("aws_virtual_hosted_style_request"));
            }
        }
    }

    @Test
    void inheritedS3EndpointsRejectInvalidBooleanValues() {
        for (String value : new String[] {"", "invalid", " true", "true "}) {
            Assertions.assertTrue(conflict(S3_URI, catalog(minio()),
                    map("virtual_hosted_style_request", value)).contains("Unrecognized value"));
        }
    }

    @Test
    void explicitVendedS3EndpointsKeepTheirNativeMeaning() {
        for (String endpointKey : new String[] {"endpoint", "aws_endpoint_url_s3"}) {
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                    catalog(minio()), map(endpointKey, "http://table.example:9000",
                            "virtual_hosted_style_request", "true"));
            Assertions.assertEquals("http://table.example:9000", options.get("aws_endpoint_url_s3"));
        }
    }

    /** Naming a dialect is a choice; the generic S3 binding is also what a heuristic match lands on. */
    @Test
    void concreteDialectWinsOverTheGenericS3Binding() {
        List<StorageProperties> storage = Arrays.asList(
                new FakeS3CompatibleStorageProperties("S3").endpoint("http://generic:9000").accessKey("g")
                        .secretKey("g").usePathStyle("true"),
                new FakeS3CompatibleStorageProperties("MINIO").endpoint("http://minio:9000").accessKey("m")
                        .secretKey("m").usePathStyle("true"));
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI, storage);
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assertions.assertEquals("m", options.get("aws_access_key_id"));
    }

    /** The provider follows the dataset URL: a local dataset gets none of the S3 options. */
    @Test
    void onlyAnS3DatasetReadsTheS3Binding() {
        Assertions.assertTrue(LanceStorageOptions.fromDorisStorageProperties("/tmp/lance/all_types.lance",
                Collections.singletonList(minio())).isEmpty());
        Assertions.assertTrue(LanceStorageOptions.fromDorisStorageProperties("file:///tmp/lance/t.lance",
                Collections.singletonList(minio())).isEmpty());
        Assertions.assertFalse(LanceStorageOptions.fromDorisStorageProperties("s3+ddb://bucket/t.lance",
                Collections.singletonList(minio())).isEmpty());
    }

    /** An explicit allow_http, from either side, is never overridden by the inferred default. */
    @Test
    void explicitAllowHttpWins() {
        Map<String, String> vended = new HashMap<>();
        vended.put("allow_http", "false");
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()), vended);
        Assertions.assertEquals("false", options.get("allow_http"));
    }

    /** What a namespace vends for a table wins, and lands on the spelling the catalog side uses. */
    @Test
    void vendedOptionsWinAndAreNormalizedOntoTheCanonicalSpelling() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("AWS_SECRET_ACCESS_KEY", "vended-sk");
        vended.put("token", "vended-token");
        vended.put("custom_option", "kept");
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()), vended);
        Assertions.assertEquals("vended-ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("vended-sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("vended-token", options.get("aws_session_token"));
        Assertions.assertEquals("kept", options.get("custom_option"));
        Assertions.assertFalse(options.containsKey("access_key_id"));
        Assertions.assertFalse(options.containsKey("token"));
    }

    @Test
    void conflictingVendedAliasesAndNulsAreRejected() {
        Map<String, String> conflicting = new HashMap<>();
        conflicting.put("access_key_id", "one");
        conflicting.put("aws_access_key_id", "two");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                        Collections.singletonList(minio()), conflicting));

        Map<String, String> withNul = new HashMap<>();
        withNul.put("aws_region", "us-east-1" + (char) 0);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                        Collections.singletonList(minio()), withNul));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                        Collections.singletonList(minio().secretKey("s" + (char) 0 + "k"))));
    }

    // ---- Vended credentials on S3: what a REST namespace hands out for one table.

    /** Spelled the way real servers spell them: both must land on one key with the namespace's value. */
    @Test
    void vendedCredentialsSupersedeTheCatalogsOnS3() {
        Map<String, String> vended = map("access_key_id", "vended-ak", "secret_access_key", "vended-sk",
                "session_token", "vended-token", "region", "eu-west-1", "virtual_hosted_style_request", "true");
        Map<String, String> merged = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                catalog(minio()), vended);
        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("vended-sk", merged.get("aws_secret_access_key"));
        Assertions.assertEquals("vended-token", merged.get("aws_session_token"));
        Assertions.assertEquals("eu-west-1", merged.get("aws_region"));
        Assertions.assertEquals("true", merged.get("aws_virtual_hosted_style_request"));
        // The inherited endpoint follows the final style, and no unprefixed twin survives.
        Assertions.assertEquals("http://warehouse.minio:9000", merged.get("aws_endpoint"));
        for (String alias : vended.keySet()) {
            Assertions.assertNull(merged.get(alias), alias + " must not survive beside its twin");
        }
    }

    /** Every accepted spelling has to collapse, or the race just moves to the ones missed. */
    @Test
    void everyS3EndpointAliasCollapsesOntoOneEntry() {
        for (String alias : new String[] {"endpoint", "endpoint_url", "aws_endpoint", "aws_endpoint_url",
                "ENDPOINT", "AWS_Endpoint_Url"}) {
            Map<String, String> merged = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                    catalog(minio()), map(alias, "http://127.0.0.1:9000"));
            Assertions.assertEquals(2, merged.keySet().stream().filter(k -> k.contains("endpoint")).count(),
                    alias + " left a competing entry");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"), alias + " did not win");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint_url_s3"));
        }
    }

    @Test
    void allowHttpIsInferredFromTheEndpointThatWon() {
        Map<String, String> https = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI, catalog(minio()),
                map("endpoint", "https://s3.example.com"));
        Assertions.assertEquals("https://s3.example.com", https.get("aws_endpoint"));
        Assertions.assertNull(https.get("allow_http"));
        Assertions.assertEquals("false", LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                catalog(minio()), map("endpoint", "http://127.0.0.1:9000", "allow_http", "false"))
                .get("allow_http"));
    }

    /** Normalizing the catalog's half infers nothing; inference runs once, on the merged map. */
    @Test
    void normalizationDoesNotInferProviderOptions() {
        Map<String, String> s3 = LanceS3StorageProvider.INSTANCE.normalizeDorisStorageOptions(S3_URI, catalog(minio()));
        Assertions.assertEquals("http://minio:9000", s3.get("aws_endpoint"));
        Assertions.assertFalse(s3.containsKey("allow_http"));
        Map<String, String> ossOptions = LanceOssStorageProvider.INSTANCE.normalizeDorisStorageOptions(
                OSS_URI, catalog(oss()));
        Assertions.assertEquals("oss-ak", ossOptions.get("oss_access_key_id"));
        Assertions.assertFalse(ossOptions.containsKey("skip_signature"));
        Assertions.assertFalse(ossOptions.containsKey("allow_anonymous"));
    }

    /** {@code token} is an S3 session token to object_store's S3 parser and a bearer token to its Azure one. */
    @Test
    void anAmbiguousTokenResolvesOnceTheProviderIsKnown() {
        List<StorageProperties> storage = catalog(minio().sessionToken("static-token"));
        Map<String, String> vended = map("token", "vended-token");
        Map<String, String> onS3 = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI, storage, vended);
        Assertions.assertEquals("vended-token", onS3.get("aws_session_token"));
        Assertions.assertNull(onS3.get("token"));
        Map<String, String> onAzure = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                "az://container/table.lance", storage, vended);
        Assertions.assertEquals("vended-token", onAzure.get("token"));
        Assertions.assertNull(onAzure.get("aws_session_token"));
    }

    /** A provider with no Doris adapter reads the namespace's options exactly as written. */
    @Test
    void datasetsOfOtherProvidersAreNeverRewritten() {
        Map<String, String> vended = map("endpoint", "http://azurite:10000", "access_key_id", "vended-ak",
                "secret_access_key", "vended-sk");
        for (String uri : new String[] {"az://container/table.lance", "abfss://fs@acct/table",
                "gs://bucket/table.lance", "cos://bucket/table", "file:///tmp/table.lance"}) {
            Assertions.assertEquals(vended, LanceStorageOptions.fromDorisAndVendedStorageOptions(uri,
                    catalog(minio()), vended), uri + " must reach Lance exactly as the namespace wrote it");
        }
    }

    /** A REST namespace, which reads no storage of its own, reports no URL and gets nothing. */
    @Test
    void catalogsWithoutAnObjectStoreUrlGetNoOptions() {
        for (String uri : new String[] {"file:///warehouse/lance", "", null}) {
            Assertions.assertTrue(LanceStorageOptions.fromDorisStorageProperties(uri, catalog(minio())).isEmpty(),
                    "expected no options for " + uri);
        }
    }

    /** The bindings follow the engine's provider registry, not the user's order, and may lead with HDFS. */
    @Test
    void selectionSkipsEntriesOfOtherKinds() {
        List<StorageProperties> storage = Arrays.asList(new FakeStorageBinding("HDFS"), minio());
        Assertions.assertEquals("ak",
                LanceStorageOptions.fromDorisStorageProperties(S3_URI, storage).get("aws_access_key_id"));
        Assertions.assertEquals("oss-ak", LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                Arrays.asList(minio(), oss())).get("oss_access_key_id"));
    }

    @Test
    void unknownVendedOptionsArePassedThroughOnS3Too() {
        // expires_at_millis is read on Lance's namespace-backed refresh path; Doris assigns it no meaning. Empty
        // is expressible as a C string, so it is carried rather than second-guessed.
        Map<String, String> vended = map("expires_at_millis", "1760000000000", "azure_storage_sas_token", "sas",
                "deliberately_empty", "");
        Assertions.assertEquals(vended, LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.emptyList(), vended));
    }

    @Test
    void absentVendedOptionsLeaveTheCatalogsIntact() {
        Map<String, String> catalogOptions = LanceStorageOptions.fromDorisStorageProperties(S3_URI, catalog(minio()));
        Assertions.assertEquals(catalogOptions, LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                catalog(minio()), new HashMap<>()));
        Assertions.assertEquals("same", LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                catalog(minio()), map("access_key_id", "same", "aws_access_key_id", "same"))
                .get("aws_access_key_id"));
    }

    @Test
    void optionsThatCannotReachTheBackendAreRejected() {
        String nul = String.valueOf((char) 0);
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .fromDorisAndVendedStorageOptions(S3_URI, Collections.emptyList(), map("bucket" + nul, "b")));
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("aws_region", null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .fromDorisAndVendedStorageOptions(S3_URI, Collections.emptyList(), nullValue));
        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "value");
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .fromDorisAndVendedStorageOptions(S3_URI, Collections.emptyList(), nullKey));
        // The message says which side to fix: the catalog, not the namespace.
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .fromDorisStorageProperties(S3_URI, catalog(minio().accessKey("ak" + nul + "x")))).getMessage()
                .contains("Doris storage configuration"));
    }

    // ---- OSS, through Lance's OpenDAL OSS provider.

    @Test
    void ossCatalogPropertiesMapToThePublicSpelling() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                catalog(oss().sessionToken("oss-token")));
        Assertions.assertEquals(map("oss_endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss_access_key_id", "oss-ak", "oss_secret_access_key", "oss-sk", "oss_region", "cn-hangzhou",
                "oss_security_token", "oss-token",
                // Stated rather than implied, so the host environment cannot decide it.
                "addressing_style", "virtual", "allow_anonymous", "false", "skip_signature", "false",
                "role_arn", ""), options);
    }

    @Test
    void ossPathStyleIsStatedEitherWay() {
        Assertions.assertEquals("path", LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                catalog(oss().usePathStyle("true"))).get("addressing_style"));
        Assertions.assertEquals("virtual", LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                catalog(oss())).get("addressing_style"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions.fromDorisStorageProperties(
                OSS_URI, catalog(oss().usePathStyle("invalid"))));
    }

    @Test
    void vendedOssAddressingStyleIsCaseInsensitive() {
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                catalog(oss()), map("ADDRESSING_STYLE", "path"));
        Assertions.assertEquals("path", options.get("addressing_style"));
        Assertions.assertFalse(options.containsKey("ADDRESSING_STYLE"));
        Assertions.assertTrue(conflict(OSS_URI, catalog(oss()),
                map("ADDRESSING_STYLE", "path", "addressing_style", "virtual"))
                .contains("conflicting values"));
    }

    @Test
    void permanentOssKeysDoNotInheritAnotherIdentitysToken() {
        Assertions.assertEquals("", LanceStorageOptions.fromDorisStorageProperties(OSS_URI, catalog(oss()))
                .get("oss_security_token"));
        for (List<StorageProperties> storage : Arrays.asList(Collections.<StorageProperties>emptyList(),
                catalog(oss().sessionToken("catalog-token")))) {
            for (Map<String, String> keys : Arrays.asList(
                    map("access_key_id", "vended-ak", "access_key_secret", "vended-sk"),
                    map("OSS_ACCESS_KEY_ID", "vended-ak", "OSS_SECRET_ACCESS_KEY", "vended-sk"))) {
                Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                        OSS_URI, storage, keys);
                Assertions.assertEquals("", options.get("oss_security_token"));
            }
        }
    }

    @Test
    void ossExplicitIdentitiesOverrideAmbientAuthenticationSelectors() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(OSS_URI, catalog(oss()));
        Assertions.assertEquals("false", options.get("skip_signature"));
        Assertions.assertEquals("", options.get("role_arn"));
        Map<String, String> vended = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                catalog(oss()), map("ROLE_ARN", "namespace-role", "security_token", "namespace-token",
                        "SKIP_SIGNATURE", "off"));
        Assertions.assertEquals("namespace-role", vended.get("role_arn"));
        Assertions.assertEquals("namespace-token", vended.get("oss_security_token"));
        Assertions.assertEquals("off", vended.get("skip_signature"));
    }

    @Test
    void ossAssumeRoleDoesNotUseTheSourceIdentity() {
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                        catalog(oss().roleArn("acs:ram::123456789012:role/lance"))))
                .getMessage().contains("OSS AssumeRole access is not supported"));
    }

    /**
     * OpenDAL skips signing only when told to, so anonymous access is stated rather than left to the
     * environment, and a credential is never emitted beside it.
     */
    @Test
    void ossAnonymousAccessIsStatedEitherWay() {
        Map<String, String> anonymous = LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                catalog(anonymousOss()));
        Assertions.assertEquals("true", anonymous.get("allow_anonymous"));
        Assertions.assertNull(anonymous.get("oss_access_key_id"));
        Assertions.assertNull(anonymous.get("oss_secret_access_key"));
        Assertions.assertNull(anonymous.get("oss_security_token"));
        Assertions.assertEquals("false", LanceStorageOptions.fromDorisStorageProperties(OSS_URI, catalog(oss()))
                .get("allow_anonymous"));
        Assertions.assertEquals(map("allow_anonymous", "true", "skip_signature", "true"),
                LanceStorageOptions.fromDorisStorageProperties(OSS_URI, null));
        // The engine accepts a whitespace key pair as both set: signing and the flag must agree.
        Map<String, String> blank = LanceStorageOptions.fromDorisStorageProperties(OSS_URI,
                catalog(anonymousOss().accessKey(" ").secretKey(" ")));
        Assertions.assertNotEquals(blank.containsKey("oss_access_key_id"),
                Boolean.parseBoolean(blank.get("allow_anonymous")));
    }

    /**
     * S3 credentials never reach an OSS dataset. The reverse is intended: an s3:// dataset reads an OSS
     * binding through OSS's S3-compatible API, as a named dialect wins over the generic S3 binding.
     */
    @Test
    void anOssDatasetNeverReadsTheS3Binding() {
        Assertions.assertEquals(map("allow_anonymous", "true", "skip_signature", "true"),
                LanceStorageOptions.fromDorisStorageProperties(OSS_URI, catalog(minio())));
        Assertions.assertEquals("oss-ak", LanceStorageOptions.fromDorisStorageProperties(S3_URI, catalog(oss()))
                .get("aws_access_key_id"));
    }

    @Test
    void vendedOssOptionsSupersedeTheCatalog() {
        Map<String, String> vended = map("endpoint", "https://oss-cn-shanghai.aliyuncs.com",
                "access_key_id", "vended-ak", "access_key_secret", "vended-sk", "region", "cn-shanghai",
                "security_token", "vended-token");
        Map<String, String> merged = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                catalog(oss().sessionToken("static-token")), vended);
        Assertions.assertEquals("https://oss-cn-shanghai.aliyuncs.com", merged.get("oss_endpoint"));
        Assertions.assertEquals("vended-ak", merged.get("oss_access_key_id"));
        Assertions.assertEquals("vended-sk", merged.get("oss_secret_access_key"));
        Assertions.assertEquals("cn-shanghai", merged.get("oss_region"));
        Assertions.assertEquals("vended-token", merged.get("oss_security_token"));
        Assertions.assertEquals("false", merged.get("allow_anonymous"));
        for (String alias : vended.keySet()) {
            Assertions.assertNull(merged.get(alias), alias + " must not survive beside its twin");
        }
    }

    /**
     * An anonymous catalog whose namespace vends real credentials: OpenDAL returns a request unsigned
     * whenever anonymous access is on, so leaving the flag on would unsign every read the credentials were
     * meant to authorize.
     */
    @Test
    void vendedOssCredentialsClearTheCatalogsAnonymousMode() {
        Map<String, String> merged = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                catalog(anonymousOss()), map("access_key_id", "vended-ak", "access_key_secret", "vended-sk"));
        Assertions.assertEquals("false", merged.get("allow_anonymous"));
        Assertions.assertEquals("vended-ak", merged.get("oss_access_key_id"));
        // OpenDAL lower-cases config keys, so an upper-case credential still signs.
        Map<String, String> upperCase = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("ACCESS_KEY_ID", "vended-ak", "Access_Key_Secret", "vended-sk"));
        Assertions.assertEquals("vended-ak", upperCase.get("oss_access_key_id"));
        Assertions.assertEquals("vended-sk", upperCase.get("oss_secret_access_key"));
        Assertions.assertEquals("false", upperCase.get("allow_anonymous"));
        // Blank is not a credential: it is carried, but the request is marked anonymous.
        Map<String, String> blank = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                catalog(oss().sessionToken("static-token")), map("access_key_id", "", "access_key_secret", ""));
        Assertions.assertEquals("true", blank.get("allow_anonymous"));
        Assertions.assertEquals("", blank.get("oss_access_key_id"));
    }

    @Test
    void incompleteVendedOssCredentialsAreRejectedBeforeAuthenticationInference() {
        for (Map<String, String> vended : List.of(
                map("access_key_id", "vended-ak"),
                map("access_key_secret", "vended-sk"),
                map("OSS_ACCESS_KEY_ID", "vended-ak", "oss_secret_access_key", ""),
                map("oss_access_key_id", "", "Access_Key_Secret", "vended-sk"),
                map("access_key_secret", "vended-sk", "allow_anonymous", "true"),
                map("access_key_id", "vended-ak", "skip_signature", "false"))) {
            for (List<StorageProperties> storage : List.of(Collections.<StorageProperties>emptyList(),
                    catalog(anonymousOss()))) {
                String message = conflict(OSS_URI, storage, vended);
                Assertions.assertTrue(message.contains("both access key and secret key"), message);
                Assertions.assertFalse(message.contains("vended-ak"));
                Assertions.assertFalse(message.contains("vended-sk"));
            }
        }
    }

    @Test
    void ossCredentialValidationUsesTheMergedPair() {
        for (Map<String, String> vended : List.of(map("access_key_id", "vended-ak"),
                map("access_key_secret", "vended-sk"))) {
            Map<String, String> merged = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                    OSS_URI, catalog(oss()), vended);
            Assertions.assertEquals(vended.getOrDefault("access_key_id", "oss-ak"), merged.get("oss_access_key_id"));
            Assertions.assertEquals(vended.getOrDefault("access_key_secret", "oss-sk"),
                    merged.get("oss_secret_access_key"));
            Assertions.assertEquals("false", merged.get("allow_anonymous"));
            Assertions.assertEquals("false", merged.get("skip_signature"));
        }
        for (String key : List.of("access_key_id", "access_key_secret")) {
            Assertions.assertTrue(conflict(OSS_URI, catalog(oss()), map(key, ""))
                    .contains("both access key and secret key"));
        }
    }

    @Test
    void ossAliasesCollapseAndConflictsAreRejected() {
        Assertions.assertEquals("same", LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("access_key_id", "ak", "access_key_secret", "same",
                        "oss_secret_access_key", "same")).get("oss_secret_access_key"));
        conflict(OSS_URI, Collections.emptyList(), map("endpoint", "https://one.example.com",
                "oss_endpoint", "https://another.example.com"));
        Assertions.assertEquals("true", LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("allow_anonymous", "true", "skip_signature", "true"))
                .get("allow_anonymous"));
        // These are separate native fields combined with OR, rather than conflicting aliases.
        Map<String, String> mixed = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("allow_anonymous", "true", "skip_signature", "false"));
        Assertions.assertEquals("true", mixed.get("allow_anonymous"));
        Assertions.assertEquals("false", mixed.get("skip_signature"));
        Map<String, String> unknown = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("unknown_provider_option", "value"));
        Assertions.assertEquals("value", unknown.get("unknown_provider_option"));
        Assertions.assertEquals("true", unknown.get("allow_anonymous"));
        Assertions.assertTrue(LanceOssStorageProvider.INSTANCE.normalizeVendedStorageOptions(null).isEmpty());
    }

    /**
     * Anonymous access beside a credential is a contradiction; refusing it beats picking one. The flag is
     * read with OpenDAL's boolean grammar, under which {@code on} also means anonymous.
     */
    @Test
    void ossAnonymousAccessBesideACredentialIsRefused() {
        for (String spelling : new String[] {"true", "on", "ON"}) {
            Assertions.assertTrue(conflict(OSS_URI, catalog(oss()), map("allow_anonymous", spelling))
                    .contains("Conflicting OSS authentication"), spelling);
        }
        Assertions.assertTrue(conflict(OSS_URI, Collections.emptyList(), map("skip_signature", "true",
                "access_key_id", "ak", "access_key_secret", "sk")).contains("Conflicting OSS authentication"));
        for (String spelling : new String[] {"false", "off"}) {
            Assertions.assertEquals(spelling, LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                    catalog(oss()), map("allow_anonymous", spelling)).get("allow_anonymous"));
        }
        Assertions.assertTrue(conflict(OSS_URI, Collections.emptyList(), map("allow_anonymous", "yes"))
                .contains("Unrecognized value"));
        Assertions.assertTrue(conflict(OSS_URI, Collections.emptyList(), map("skip_signature", "yes"))
                .contains("Unrecognized value"));
    }

    @Test
    void ossExplicitSigningChoiceWinsAndInferenceIsIdempotent() {
        Map<String, String> explicit = LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), map("skip_signature", "false"));
        Assertions.assertEquals("false", explicit.get("allow_anonymous"));
        Assertions.assertEquals("false", explicit.get("skip_signature"));
        Map<String, String> inferred = LanceStorageOptions.fromDorisStorageProperties(OSS_URI, catalog(oss()));
        Assertions.assertEquals(inferred, LanceStorageOptions.fromDorisAndVendedStorageOptions(OSS_URI,
                Collections.emptyList(), inferred));
    }

    /** The engine binds OSS-HDFS as HDFS-compatible storage, which carries no Lance OSS options. */
    @Test
    void ossHdfsBindingsAreRefused() {
        LanceStorageOptions.rejectOssHdfs(Arrays.asList(minio(), oss()));
        IllegalArgumentException thrown = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.rejectOssHdfs(Arrays.asList(minio(), new FakeStorageBinding("OSS_HDFS"))));
        Assertions.assertTrue(thrown.getMessage().startsWith("OSS-HDFS is not supported by the Lance catalog"));
    }
}
