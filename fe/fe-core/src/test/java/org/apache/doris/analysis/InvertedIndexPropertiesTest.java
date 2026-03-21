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

package org.apache.doris.analysis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class InvertedIndexPropertiesTest {

    // --- getInvertedIndexParser ---

    @Test
    public void testGetParserNull() {
        Assertions.assertEquals("none", InvertedIndexProperties.getInvertedIndexParser(null));
    }

    @Test
    public void testGetParserEmpty() {
        Assertions.assertEquals("none", InvertedIndexProperties.getInvertedIndexParser(new HashMap<>()));
    }

    @Test
    public void testGetParserByKey() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "unicode");
        Assertions.assertEquals("unicode", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    @Test
    public void testGetParserByAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("built_in_analyzer", "chinese");
        Assertions.assertEquals("chinese", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    @Test
    public void testGetParserKeyTakesPrecedenceOverAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "standard");
        props.put("built_in_analyzer", "chinese");
        Assertions.assertEquals("standard", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    // --- getInvertedIndexParserMode ---

    @Test
    public void testGetParserModeNull() {
        Assertions.assertEquals("coarse_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(null));
    }

    @Test
    public void testGetParserModeExplicit() {
        Map<String, String> props = new HashMap<>();
        props.put("parser_mode", "fine_grained");
        Assertions.assertEquals("fine_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForIk() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "ik");
        Assertions.assertEquals("ik_smart",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForNonIk() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "unicode");
        Assertions.assertEquals("coarse_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    // --- getInvertedIndexFieldPattern ---

    @Test
    public void testGetFieldPatternNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getInvertedIndexFieldPattern(null));
    }

    @Test
    public void testGetFieldPatternSet() {
        Map<String, String> props = new HashMap<>();
        props.put("field_pattern", "\\d+");
        Assertions.assertEquals("\\d+", InvertedIndexProperties.getInvertedIndexFieldPattern(props));
    }

    // --- getInvertedIndexParserLowercase ---

    @Test
    public void testGetLowercaseNull() {
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexParserLowercase(null));
    }

    @Test
    public void testGetLowercaseDefault() {
        Assertions.assertTrue(
                InvertedIndexProperties.getInvertedIndexParserLowercase(new HashMap<>()));
    }

    @Test
    public void testGetLowercaseFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("lower_case", "false");
        Assertions.assertFalse(InvertedIndexProperties.getInvertedIndexParserLowercase(props));
    }

    // --- getInvertedIndexParserStopwords ---

    @Test
    public void testGetStopwordsNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getInvertedIndexParserStopwords(null));
    }

    @Test
    public void testGetStopwordsSet() {
        Map<String, String> props = new HashMap<>();
        props.put("stopwords", "the,a,an");
        Assertions.assertEquals("the,a,an",
                InvertedIndexProperties.getInvertedIndexParserStopwords(props));
    }

    // --- getPreferredAnalyzer ---

    @Test
    public void testGetPreferredAnalyzerNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getPreferredAnalyzer(null));
    }

    @Test
    public void testGetPreferredAnalyzerEmpty() {
        Assertions.assertEquals("", InvertedIndexProperties.getPreferredAnalyzer(new HashMap<>()));
    }

    @Test
    public void testGetPreferredAnalyzerByAnalyzer() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "my_analyzer");
        Assertions.assertEquals("my_analyzer", InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    @Test
    public void testGetPreferredAnalyzerByNormalizer() {
        Map<String, String> props = new HashMap<>();
        props.put("normalizer", "my_normalizer");
        Assertions.assertEquals("my_normalizer",
                InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    @Test
    public void testGetPreferredAnalyzerPrecedence() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "my_analyzer");
        props.put("normalizer", "my_normalizer");
        Assertions.assertEquals("my_analyzer", InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    // --- getInvertedIndexCharFilter ---

    @Test
    public void testGetCharFilterNull() {
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(null).isEmpty());
    }

    @Test
    public void testGetCharFilterNoType() {
        Map<String, String> props = new HashMap<>();
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterUnknownType() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "unknown_type");
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterCharReplaceNoPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterCharReplaceWithPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "[0-9]");
        Map<String, String> result = InvertedIndexProperties.getInvertedIndexCharFilter(props);
        Assertions.assertEquals("char_replace", result.get("char_filter_type"));
        Assertions.assertEquals("[0-9]", result.get("char_filter_pattern"));
        Assertions.assertEquals(" ", result.get("char_filter_replacement"));
    }

    @Test
    public void testGetCharFilterCharReplaceWithCustomReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "[0-9]");
        props.put("char_filter_replacement", "_");
        Map<String, String> result = InvertedIndexProperties.getInvertedIndexCharFilter(props);
        Assertions.assertEquals("_", result.get("char_filter_replacement"));
    }

    // --- buildAnalyzerSqlFragment (migrated from InvertedIndexSqlGeneratorTest) ---

    @Test
    public void testBuildAnalyzerSqlFragmentNullOrBlank() {
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment(null));
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment(""));
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment("   "));
    }

    @Test
    public void testBuildAnalyzerSqlFragmentIdentifier() {
        Assertions.assertEquals(" USING ANALYZER standard",
                InvertedIndexProperties.buildAnalyzerSqlFragment("standard"));
        Assertions.assertEquals(" USING ANALYZER foo_bar",
                InvertedIndexProperties.buildAnalyzerSqlFragment("foo_bar"));
    }

    @Test
    public void testBuildAnalyzerSqlFragmentQuoted() {
        Assertions.assertEquals(" USING ANALYZER 'foo bar'",
                InvertedIndexProperties.buildAnalyzerSqlFragment("foo bar"));
        Assertions.assertEquals(" USING ANALYZER 'O''Reilly'",
                InvertedIndexProperties.buildAnalyzerSqlFragment("O'Reilly"));
    }

}
