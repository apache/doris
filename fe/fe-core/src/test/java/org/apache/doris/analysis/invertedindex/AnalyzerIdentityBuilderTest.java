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

package org.apache.doris.analysis.invertedindex;

import org.apache.doris.indexpolicy.IndexPolicy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AnalyzerIdentityBuilderTest {

    private Map<String, String> nonEmptyProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k", "v");
        return properties;
    }

    @Test
    public void testEmptyPropertiesReturnsDefault() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                new HashMap<>(),
                "",
                "",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("__default__", identity);
    }

    @Test
    public void testBuiltInAnalyzerPreferred() {
        Assertions.assertFalse(IndexPolicy.BUILTIN_ANALYZERS.isEmpty());
        Iterator<String> iterator = IndexPolicy.BUILTIN_ANALYZERS.iterator();
        String analyzer = iterator.next();

        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                analyzer,
                "",
                "__default__",
                "none",
                null);
        Assertions.assertEquals(analyzer, identity);
    }

    @Test
    public void testBuiltInNormalizerPreferred() {
        Assertions.assertFalse(IndexPolicy.BUILTIN_NORMALIZERS.isEmpty());
        Iterator<String> iterator = IndexPolicy.BUILTIN_NORMALIZERS.iterator();
        String normalizer = iterator.next();

        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                normalizer,
                "",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("normalizer:" + normalizer, identity);
    }

    @Test
    public void testParserNoneReturnsDefault() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                "",
                "none",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("__default__", identity);
    }

    @Test
    public void testParserReturnsParserName() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                "",
                "standard",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("standard", identity);
    }
}
