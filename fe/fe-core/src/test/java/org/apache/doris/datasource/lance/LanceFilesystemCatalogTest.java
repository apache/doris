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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceFilesystemCatalogTest {

    @Test
    public void testMinioStorageOptionMapping() {
        Map<String, String> backendProperties = new HashMap<>();
        backendProperties.put("AWS_ACCESS_KEY", "ak");
        backendProperties.put("AWS_SECRET_KEY", "sk");
        backendProperties.put("AWS_ENDPOINT", "http://minio:9000");
        backendProperties.put("AWS_REGION", "us-east-1");
        backendProperties.put("use_path_style", "true");

        Map<String, String> options = LanceStorageOptions.forJavaSdk(backendProperties);
        Assert.assertEquals("ak", options.get("aws_access_key_id"));
        Assert.assertEquals("sk", options.get("aws_secret_access_key"));
        Assert.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assert.assertEquals("us-east-1", options.get("aws_region"));
        Assert.assertEquals("true", options.get("allow_http"));
        Assert.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
    }

    @Test
    public void testNamespaceNameRoundTrip() throws Exception {
        Assert.assertEquals(Collections.emptyList(), LanceNamespaceName.dorisDatabaseNameToNamespace(
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.emptyList(), ".", "default"),
                ".", "default"));
        Assert.assertEquals("doris",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.singletonList("doris"), ".", "default"));
        Assert.assertEquals("company.analytics",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        java.util.Arrays.asList("company", "analytics"), ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("company", "analytics"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("company", "analytics"), ".", "default"),
                        ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a.b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a.b", "c"), ".", "default"),
                        ".", "default"));

        java.util.List<String> delimiterAtEnd = java.util.Arrays.asList("a.", "b");
        java.util.List<String> delimiterAtStart = java.util.Arrays.asList("a", ".b");
        String encodedAtEnd =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtEnd, ".", "default");
        String encodedAtStart =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtStart, ".", "default");
        Assert.assertNotEquals(encodedAtEnd, encodedAtStart);
        Assert.assertEquals(delimiterAtEnd,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtEnd, ".", "default"));
        Assert.assertEquals(delimiterAtStart,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtStart, ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a\\b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a\\b", "c"), ".", "default"),
                        ".", "default"));

        String rootCollision = LanceNamespaceName.namespaceToDorisDatabaseName(
                Collections.singletonList("default"), ".", "default");
        Assert.assertEquals("\\default", rootCollision);
        Assert.assertEquals(Collections.singletonList("default"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(rootCollision, ".", "default"));
    }
}
