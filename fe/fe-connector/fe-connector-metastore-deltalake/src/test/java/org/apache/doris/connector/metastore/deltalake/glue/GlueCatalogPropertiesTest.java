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

package org.apache.doris.connector.metastore.deltalake.glue;

import org.apache.doris.connector.metastore.spi.MetaStoreProviders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

class GlueCatalogPropertiesTest {
    private static GlueCatalogProperties bind(Map<String, String> raw) {
        var holder = (GlueCatalogProperties) MetaStoreProviders.bindForType("gLuE", raw, Map.of());
        holder.validate();
        return holder;
    }

    @Test
    void discoversGlueAndResolvesEndpointAndRegionAliases() {
        Assertions.assertTrue(MetaStoreProviders.registeredNames().contains("GLUE"));
        for (String key : List.of("glue.endpoint", "aws.endpoint", "aws.glue.endpoint")) {
            var props = bind(Map.of(key, "https://glue.eu-west-1.amazonaws.com/"));
            Assertions.assertEquals("GLUE", props.providerName());
            Assertions.assertTrue(props.needsStorage());
            Assertions.assertEquals(URI.create("https://glue.eu-west-1.amazonaws.com/"), props.endpoint());
            Assertions.assertEquals("eu-west-1", props.region());
        }
        for (String key : List.of("glue.region", "aws.region", "aws.glue.region")) {
            var props = bind(Map.of(key, "us-east-2"));
            Assertions.assertNull(props.endpoint());
            Assertions.assertEquals("us-east-2", props.region());
        }
        Assertions.assertEquals("us-gov-west-1",
                bind(Map.of("glue.endpoint", "https://glue-fips.us-gov-west-1.api.aws")).region());
        Assertions.assertEquals("cn-north-1",
                bind(Map.of("glue.endpoint", "https://glue.cn-north-1.amazonaws.com.cn")).region());
        Assertions.assertEquals("us-west-1", bind(Map.of("glue.region", "us-west-1",
                "glue.endpoint", "https://glue.us-west-2.amazonaws.com")).region());
        Assertions.assertEquals(URI.create("http://localhost:1234/catalog"), bind(Map.of(
                "glue.region", "us-east-1", "glue.endpoint", "http://localhost:1234/catalog")).endpoint());
    }

    @Test
    void bindsAndMasksEveryCredentialAlias() {
        for (String ak : List.of("glue.access_key", "aws.glue.access-key")) {
            for (String sk : List.of("glue.secret_key", "aws.glue.secret-key")) {
                for (String token : List.of("glue.session_token", "aws.glue.session-token")) {
                    var props = bind(Map.of("glue.region", "us-east-1", ak, "test-ak", sk, "test-sk", token, "test-st",
                            "glue.role_arn", "arn:aws:iam::123456789012:role/test", "glue.external_id", "test-id",
                            "glue.catalog_id", "210987654321"));
                    Assertions.assertEquals("test-ak", props.accessKey());
                    Assertions.assertEquals("test-sk", props.secretKey());
                    Assertions.assertEquals("test-st", props.sessionToken());
                    Assertions.assertEquals("arn:aws:iam::123456789012:role/test", props.roleArn());
                    Assertions.assertEquals("test-id", props.externalId());
                    Assertions.assertEquals("210987654321", props.catalogId());
                    Assertions.assertTrue(MetaStoreProviders.sensitivePropertyKeys()
                            .containsAll(List.of(ak, sk, token)));
                }
            }
        }
        var defaults = bind(Map.of("glue.region", "us-east-1"));
        Assertions.assertNull(defaults.accessKey());
        Assertions.assertNull(defaults.roleArn());
        Assertions.assertNull(defaults.catalogId());
        Assertions.assertNull(bind(Map.of("glue.region", "us-east-1", "glue.role_arn", "role")).accessKey());
    }

    @Test
    void rejectsInvalidEndpointsAndUnresolvedRegions() {
        for (String endpoint : List.of("glue.us-east-1.amazonaws.com", "ftp://example.test", "http:///missing-host",
                "http://bad host", "https://example.test:90000", "http://user@example.test", "https://example.test?q=x",
                "https://example.test/#fragment")) {
            Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                    () -> bind(Map.of("glue.endpoint", endpoint, "glue.region", "us-east-1")))
                    .getMessage().contains("glue.endpoint"), endpoint);
        }
        for (var raw : List.of(Map.<String, String>of(), Map.of("aws.endpoint", "http://localhost:1234"),
                Map.of("aws.glue.endpoint", "https://glue.us-east-1.amazonaws.com.example.test"))) {
            Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> bind(raw))
                    .getMessage().contains("glue.region"));
        }
    }

    @Test
    void rejectsPartialCredentialsAndExternalIdWithoutARole() {
        for (var pair : Map.of("glue.access_key", "glue.secret_key", "aws.glue.secret-key", "glue.access_key",
                "aws.glue.session-token", "glue.session_token", "glue.external_id", "glue.role_arn").entrySet()) {
            Map<String, String> raw = Map.of("glue.region", "us-east-1", pair.getKey(), "value");
            Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> bind(raw))
                    .getMessage().contains(pair.getValue()), pair.getKey());
        }
    }
}
