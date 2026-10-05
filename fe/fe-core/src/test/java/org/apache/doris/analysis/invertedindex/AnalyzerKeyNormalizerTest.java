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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AnalyzerKeyNormalizerTest {

    @Test
    public void testNullPropertiesNoOp() {
        // Should not throw when properties is null
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(null, "key");
    }

    @Test
    public void testNullKeysNoOp() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "CHINESE");
        // Should not throw when keys is null
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, (String[]) null);
        // Value should remain unchanged
        Assertions.assertEquals("CHINESE", props.get("analyzer"));
    }

    @Test
    public void testNormalizesToLowercase() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "CHINESE");
        props.put("parser", "STANDARD");
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer", "parser");
        Assertions.assertEquals("chinese", props.get("analyzer"));
        Assertions.assertEquals("standard", props.get("parser"));
    }

    @Test
    public void testTrimsWhitespace() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "  standard  ");
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "parser");
        Assertions.assertEquals("standard", props.get("parser"));
    }

    @Test
    public void testMixedCaseAndWhitespace() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "  ChInEsE  ");
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer");
        Assertions.assertEquals("chinese", props.get("analyzer"));
    }

    @Test
    public void testEmptyValueUnchanged() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "");
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer");
        Assertions.assertEquals("", props.get("analyzer"));
    }

    @Test
    public void testMissingKeyNoOp() {
        Map<String, String> props = new HashMap<>();
        props.put("other_key", "VALUE");
        // Normalizing a key that doesn't exist should not throw
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer");
        // Other keys should remain unchanged
        Assertions.assertEquals("VALUE", props.get("other_key"));
        Assertions.assertNull(props.get("analyzer"));
    }

    @Test
    public void testMultipleKeysPartialMatch() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "CHINESE");
        // parser key doesn't exist
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer", "parser");
        Assertions.assertEquals("chinese", props.get("analyzer"));
        Assertions.assertNull(props.get("parser"));
    }

    @Test
    public void testNullKeyInArray() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "CHINESE");
        // Should handle null key in the array gracefully
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(props, "analyzer", null, "parser");
        Assertions.assertEquals("chinese", props.get("analyzer"));
    }
}
