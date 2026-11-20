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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class LanceNamespaceNameTest {

    @Test
    void namespaceNameRoundTrip() {
        Assertions.assertEquals(Collections.emptyList(), LanceNamespaceName.dorisDatabaseNameToNamespace(
                LanceNamespaceName.namespaceToDorisDatabaseName(Collections.emptyList(), ".", "default"),
                ".", "default"));
        Assertions.assertEquals("doris",
                LanceNamespaceName.namespaceToDorisDatabaseName(Collections.singletonList("doris"), ".", "default"));
        Assertions.assertEquals("company.analytics",
                LanceNamespaceName.namespaceToDorisDatabaseName(Arrays.asList("company", "analytics"), ".",
                        "default"));
        Assertions.assertEquals(Arrays.asList("company", "analytics"), roundTrip(Arrays.asList("company", "analytics")));
        Assertions.assertEquals(Arrays.asList("a.b", "c"), roundTrip(Arrays.asList("a.b", "c")));

        List<String> delimiterAtEnd = Arrays.asList("a.", "b");
        List<String> delimiterAtStart = Arrays.asList("a", ".b");
        String encodedAtEnd = LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtEnd, ".", "default");
        String encodedAtStart = LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtStart, ".", "default");
        Assertions.assertNotEquals(encodedAtEnd, encodedAtStart);
        Assertions.assertEquals(delimiterAtEnd,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtEnd, ".", "default"));
        Assertions.assertEquals(delimiterAtStart,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtStart, ".", "default"));
        Assertions.assertEquals(Arrays.asList("a\\b", "c"), roundTrip(Arrays.asList("a\\b", "c")));

        String rootCollision = LanceNamespaceName.namespaceToDorisDatabaseName(
                Collections.singletonList("default"), ".", "default");
        Assertions.assertEquals("default.\\", rootCollision);
        Assertions.assertEquals(Collections.singletonList("default"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(rootCollision, ".", "default"));
    }

    @Test
    void customRootAliasesDoNotConsumeComponentEscapes() {
        for (String root : Arrays.asList(".analytics", "\\analytics", "a.b", "a\\.b", "default", "trailing\\")) {
            for (List<String> namespace : Arrays.asList(Collections.singletonList(root),
                    Arrays.asList("a", "b"), Collections.singletonList("a.b"),
                    Collections.singletonList("\\analytics"), Collections.singletonList("default"))) {
                String encoded = LanceNamespaceName.namespaceToDorisDatabaseName(namespace, ".", root);
                Assertions.assertNotEquals(root, encoded);
                Assertions.assertEquals(namespace,
                        LanceNamespaceName.dorisDatabaseNameToNamespace(encoded, ".", root), root + ": " + encoded);
            }
        }
    }

    @Test
    void parentNamespaceUsesTheConfiguredDelimiterAndRejectsMalformedEscapes() {
        Assertions.assertEquals(Collections.emptyList(), LanceNamespaceName.parseParentNamespace("", "$"));
        Assertions.assertEquals(Arrays.asList("company", "analytics"),
                LanceNamespaceName.parseParentNamespace("company$analytics", "$"));
        Assertions.assertEquals(Collections.singletonList("a$b"),
                LanceNamespaceName.parseParentNamespace("a\\$b", "$"));
        Assertions.assertEquals("Invalid escape in Lance namespace 'a\\b'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceNamespaceName.parseParentNamespace("a\\b", "$")).getMessage());
        Assertions.assertEquals("Empty Lance namespace component in 'a$$b'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceNamespaceName.parseParentNamespace("a$$b", "$")).getMessage());
    }

    private static List<String> roundTrip(List<String> namespace) {
        return LanceNamespaceName.dorisDatabaseNameToNamespace(
                LanceNamespaceName.namespaceToDorisDatabaseName(namespace, ".", "default"), ".", "default");
    }
}
