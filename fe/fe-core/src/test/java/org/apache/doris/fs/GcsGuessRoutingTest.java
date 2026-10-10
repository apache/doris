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

package org.apache.doris.fs;

import org.apache.doris.datasource.storage.StorageAdapter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GcsGuessRoutingTest {
    private static final String[] ENDPOINTS = {
            "https://storage.googleapis.com", "https://storage.googleapis.com:443",
            "https://storage.googleapis.com/", "storage.googleapis.com:443/",
            "https://storage.us-east1.rep.googleapis.com", "https://us-east1-storage.googleapis.com"};

    @Test
    public void testExplicitS3KeepsAwsAuthenticationInPrimaryAndAllBindings() {
        for (String endpoint : ENDPOINTS) {
            for (String provider : new String[] {"S3", "s3"}) {
                Map<String, String> props = properties(endpoint);
                props.put("provider", provider);
                assertS3Bindings(props, "DEFAULT");
                props.put("s3.access_key", "ak");
                props.put("s3.secret_key", "sk");
                assertS3Bindings(props, "DEFAULT");
                props.remove("s3.access_key");
                props.remove("s3.secret_key");
                props.put("s3.credentials_provider_type", "ANONYMOUS");
                assertS3Bindings(props, "ANONYMOUS");
            }
        }
    }

    @Test
    public void testInferredGcsEndpointsBindNativeAndHmacWithoutMinio() {
        for (String endpoint : ENDPOINTS) {
            Map<String, String> props = properties(endpoint);
            for (StorageAdapter adapter : bindings(props, "GCS")) {
                Assertions.assertEquals("DEFAULT",
                        adapter.getBackendConfigProperties().get("gs.credential_provider_type"));
            }
            props.put("s3.access_key", "ak");
            props.put("s3.secret_key", "sk");
            for (StorageAdapter adapter : bindings(props, "GCS")) {
                Assertions.assertFalse(adapter.getBackendConfigProperties().containsKey("gs.credential_provider_type"));
                Assertions.assertEquals("ak", adapter.getBackendConfigProperties().get("AWS_ACCESS_KEY"));
            }
        }
    }

    @Test
    public void testInferredNativeGcsStillValidatesEndpoint() {
        for (String endpoint : new String[] {"http://storage.googleapis.com",
                "https://storage.googleapis.com:8443", "https://storage.googleapis.com/bucket",
                "https://storage.googleapis.com?query=value", "https://bucket.storage.googleapis.com"}) {
            Map<String, String> props = properties(endpoint);
            Assertions.assertThrows(IllegalArgumentException.class, () -> StorageAdapter.of(props), endpoint);
            Assertions.assertThrows(IllegalArgumentException.class, () -> StorageAdapter.ofAll(props), endpoint);
        }
    }

    private static Map<String, String> properties(String endpoint) {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", endpoint);
        props.put("s3.region", "us-east1");
        props.put("uri", "s3://bucket/key");
        return props;
    }

    private static void assertS3Bindings(Map<String, String> props, String mode) {
        for (StorageAdapter adapter : bindings(props, "S3")) {
            Map<String, String> backend = adapter.getBackendConfigProperties();
            Assertions.assertEquals(mode, backend.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
            Assertions.assertFalse(backend.containsKey("gs.credential_provider_type"));
        }
    }

    private static List<StorageAdapter> bindings(Map<String, String> props, String expectedProvider) {
        StorageAdapter primary = StorageAdapter.of(props);
        Assertions.assertEquals(expectedProvider, primary.getSpiProperties().providerName());
        // bindAll also supplies the default HDFS filesystem for catalog callers.
        List<StorageAdapter> objectBindings = StorageAdapter.ofAll(props).stream()
                .filter(adapter -> !"HDFS".equals(adapter.getStorageName())).collect(Collectors.toList());
        Assertions.assertEquals(1, objectBindings.size(), props.toString());
        Assertions.assertEquals(expectedProvider, objectBindings.get(0).getSpiProperties().providerName());
        return List.of(primary, objectBindings.get(0));
    }
}
