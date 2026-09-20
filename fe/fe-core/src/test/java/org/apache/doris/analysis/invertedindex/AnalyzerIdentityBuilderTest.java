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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
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

    @Test
    public void testNgramValidationLimitDoesNotChangeAnalyzerIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Map<String, String> tokenizerProps = new HashMap<>();
        tokenizerProps.put(IndexPolicy.PROP_TYPE, "ngram");
        tokenizerProps.put("min_gram", "1");
        tokenizerProps.put("max_gram", "2");
        tokenizerProps.put("max_ngram_diff", "7");
        IndexPolicy tokenizerWithLimit = new IndexPolicy(
                1, "ngram_with_limit", IndexPolicyTypeEnum.TOKENIZER, tokenizerProps);

        Map<String, String> equivalentTokenizerProps = new HashMap<>(tokenizerProps);
        equivalentTokenizerProps.remove("max_ngram_diff");
        IndexPolicy tokenizerWithoutLimit = new IndexPolicy(
                2, "ngram_without_limit", IndexPolicyTypeEnum.TOKENIZER, equivalentTokenizerProps);

        IndexPolicy analyzerWithLimit = analyzerPolicy(3, "analyzer_with_limit", "ngram_with_limit");
        IndexPolicy analyzerWithoutLimit = analyzerPolicy(4, "analyzer_without_limit", "ngram_without_limit");
        Mockito.when(policyMgr.getPolicyByName("ngram_with_limit")).thenReturn(tokenizerWithLimit);
        Mockito.when(policyMgr.getPolicyByName("ngram_without_limit")).thenReturn(tokenizerWithoutLimit);
        Mockito.when(policyMgr.getPolicyByName("analyzer_with_limit")).thenReturn(analyzerWithLimit);
        Mockito.when(policyMgr.getPolicyByName("analyzer_without_limit")).thenReturn(analyzerWithoutLimit);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String identityWithLimit = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "analyzer_with_limit", "", "__default__", "none", null);
            String identityWithoutLimit = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "analyzer_without_limit", "", "__default__", "none", null);
            Assertions.assertEquals(identityWithoutLimit, identityWithLimit);
        }
    }

    @Test
    public void testReplayedInvalidNgramDoesNotBlockValidReplacement() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Map<String, String> invalidProps = new HashMap<>();
        invalidProps.put(IndexPolicy.PROP_TYPE, "ngram");
        invalidProps.put("min_gram", "1");
        invalidProps.put("max_gram", "8");
        IndexPolicy invalidTokenizer = new IndexPolicy(
                10, "replayed_ngram", IndexPolicyTypeEnum.TOKENIZER, invalidProps);

        Map<String, String> replacementProps = new HashMap<>(invalidProps);
        replacementProps.put("max_ngram_diff", "7");
        IndexPolicy replacementTokenizer = new IndexPolicy(
                11, "replacement_ngram", IndexPolicyTypeEnum.TOKENIZER, replacementProps);
        IndexPolicy invalidAnalyzer = analyzerPolicy(12, "replayed_analyzer", "replayed_ngram");
        IndexPolicy replacementAnalyzer = analyzerPolicy(13, "replacement_analyzer", "replacement_ngram");
        policyMgr.replayCreateIndexPolicy(invalidTokenizer);
        policyMgr.replayCreateIndexPolicy(replacementTokenizer);
        policyMgr.replayCreateIndexPolicy(invalidAnalyzer);
        policyMgr.replayCreateIndexPolicy(replacementAnalyzer);

        Assertions.assertTrue(invalidTokenizer.isInvalid());
        Assertions.assertFalse(replacementTokenizer.isInvalid());
        Assertions.assertThrows(DdlException.class,
                () -> policyMgr.validateAnalyzerExists("replayed_analyzer"));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String invalidIdentity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "replayed_analyzer", "", "__default__", "none", null);
            String replacementIdentity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "replacement_analyzer", "", "__default__", "none", null);
            Assertions.assertNotEquals(invalidIdentity, replacementIdentity);
        }
    }

    @Test
    public void testReplayedLegacyLargeNgramAnalyzerRemainsUsable() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Map<String, String> legacyProps = new HashMap<>();
        legacyProps.put(IndexPolicy.PROP_TYPE, "ngram");
        legacyProps.put("min_gram", "2048");
        legacyProps.put("max_gram", "2048");
        IndexPolicy legacyTokenizer = new IndexPolicy(
                20, "legacy_large_ngram", IndexPolicyTypeEnum.TOKENIZER, legacyProps);
        IndexPolicy legacyAnalyzer = analyzerPolicy(
                21, "legacy_large_analyzer", "legacy_large_ngram");

        policyMgr.replayCreateIndexPolicy(legacyTokenizer);
        policyMgr.replayCreateIndexPolicy(legacyAnalyzer);

        Assertions.assertFalse(legacyTokenizer.isInvalid());
        Assertions.assertDoesNotThrow(
                () -> policyMgr.validateAnalyzerExists("legacy_large_analyzer"));
    }

    private IndexPolicy analyzerPolicy(long id, String name, String tokenizer) {
        Map<String, String> properties = new HashMap<>();
        properties.put(IndexPolicy.PROP_TOKENIZER, tokenizer);
        return new IndexPolicy(id, name, IndexPolicyTypeEnum.ANALYZER, properties);
    }

    @Test
    public void testReplayedExactIkAnalyzerUsesCustomIdentity() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(30, "IK", "standard"));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(31, "equivalent_standard", "standard"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String exact = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "IK"), "IK", "none", "__default__", "none", null);
            String equivalent = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "equivalent_standard"), "equivalent_standard",
                    "none", "__default__", "none", null);
            String builtin = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(equivalent, exact),
                    () -> Assertions.assertNotEquals(builtin, exact));
        }
    }

    @Test
    public void testNamedCharReplaceIdentityUsesEffectiveByteSet() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(50, "plain_keyword", "keyword"));
        String[] patterns = {"ab", "ba", "aabx", "x", " "};
        for (int i = 0; i < patterns.length; ++i) {
            String filter = "byte_filter_" + i;
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "char_replace");
            properties.put("pattern", patterns[i]);
            if (i < 4) {
                properties.put("replacement", "x");
            }
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    51 + i, filter, IndexPolicyTypeEnum.CHAR_FILTER, properties));
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    61 + i, "filtered_keyword_" + i, IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", filter)));
        }

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String canonical = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "filtered_keyword_0", "none", "__default__", "none", null);
            String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "plain_keyword", "none", "__default__", "none", null);
            Assertions.assertNotEquals(plain, canonical);
            for (int i = 1; i < patterns.length; ++i) {
                String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "filtered_keyword_" + i, "none", "__default__", "none", null);
                Assertions.assertEquals(i < 3 ? canonical : plain, identity);
            }
        }
    }

    @Test
    public void testNamedCharReplaceIdentityAccountsForIkLowercase() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                70, "ascii_lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(71, "plain_smart", "ik_smart"));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                72, "filtered_smart", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "ascii_lower_a")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "plain_smart", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "filtered_smart", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testNamedCharReplaceContextUsesResolvedTokenizerAndFilterOrder() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                80, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                81, "a_to_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "a", "replacement", "b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                82, "named_max_word", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "ik_max_word")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                83, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword")));
        long analyzerId = 84;
        for (String tokenizer : new String[] {"named_max_word", "ik_smart"}) {
            policyMgr.replayCreateIndexPolicy(analyzerPolicy(analyzerId++, "plain_" + tokenizer, tokenizer));
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    analyzerId++, "filtered_" + tokenizer, IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", tokenizer, "char_filter", "lower_a")));
        }
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                88, "ordered", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "named_max_word", "char_filter", "lower_a,a_to_b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                89, "later_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "named_max_word", "char_filter", "a_to_b")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (String tokenizer : new String[] {"named_max_word", "ik_smart"}) {
                String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "plain_" + tokenizer, "none", "__default__", "none", null);
                String filtered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "filtered_" + tokenizer, "none", "__default__", "none", null);
                Assertions.assertEquals("named_max_word".equals(tokenizer), plain.equals(filtered));
            }
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "ordered", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "later_only", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testCaseFoldingCharFilterAbsorbsEarlierReplacement() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                90, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                91, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                92, "compose_only", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfc")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                93, "filtered_fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[a-z]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                94, "fold_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                95, "lower_then_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                96, "compose_only_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "compose_only")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                97, "lower_then_compose", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,compose_only")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                98, "filtered_fold_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "filtered_fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                99, "lower_then_filtered_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,filtered_fold")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "fold_only", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_fold", "none", "__default__", "none", null));
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "compose_only_analyzer", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_compose", "none", "__default__", "none", null));
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "filtered_fold_analyzer", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_filtered_fold", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testBuiltinTokenizerIdentityIsCanonicalized() throws Exception {
        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        Assertions.assertEquals("ik_smart",
                resolve.invoke(null, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
        Assertions.assertEquals("ik_smart",
                resolve.invoke(null, " IK_SMART ", IndexPolicyTypeEnum.TOKENIZER));
    }

    @Test
    public void testBuiltinFilterIdentitiesAreCanonicalized() throws Exception {
        Method resolveTokenFilters = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveTokenFilterIdentity", String.class);
        resolveTokenFilters.setAccessible(true);
        Method resolveCharFilters = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveCharFilterIdentity", String.class);
        resolveCharFilters.setAccessible(true);

        Assertions.assertEquals("pinyin", resolveTokenFilters.invoke(null, "PINYIN"));
        Assertions.assertEquals("icu_normalizer", resolveCharFilters.invoke(null, "ICU_NORMALIZER"));
        Assertions.assertEquals("lowercase,pinyin",
                resolveTokenFilters.invoke(null, "empty, lowercase, empty, pinyin"));
        Assertions.assertEquals("char_replace",
                resolveCharFilters.invoke(null, "empty, char_replace, empty"));
    }

    @Test
    public void testTypeOnlyIkPolicyMatchesBuiltinIdentity() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("named_ik")).thenReturn(new IndexPolicy(
                1, "named_ik", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(resolve.invoke(null, "ik_smart", IndexPolicyTypeEnum.TOKENIZER),
                    resolve.invoke(null, "named_ik", IndexPolicyTypeEnum.TOKENIZER));
        }
    }

    @Test
    public void testNamedEmptyFiltersAreOmittedFromIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("empty_token_filter")).thenReturn(new IndexPolicy(
                1, "empty_token_filter", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "empty")));
        Mockito.when(policyMgr.getPolicyByName("empty_char_filter")).thenReturn(new IndexPolicy(
                2, "empty_char_filter", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "empty")));
        Mockito.when(policyMgr.getPolicyByName("plain")).thenReturn(new IndexPolicy(
                3, "plain", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("padded")).thenReturn(new IndexPolicy(
                4, "padded", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "token_filter", "empty_token_filter,empty",
                        "char_filter", "empty,empty_char_filter")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            Map.of("analyzer", "plain"), "plain", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            Map.of("analyzer", "padded"), "padded", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testOuterCharFilterDistinguishesNamedAnalyzerIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart")).thenReturn(new IndexPolicy(
                1, "smart", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart"), "smart", "none", "__default__", "none", null);
            String filtered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "smart", "none", "__default__", "none", null);
            String defaultReplacement = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertNotEquals(plain, filtered);
            Assertions.assertEquals(filtered, defaultReplacement);
        }
    }

    @Test
    public void testOuterCharFilterUsesCanonicalIkBaseAndByteSetSemantics() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart")).thenReturn(new IndexPolicy(
                1, "smart", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("shadowed_tokenizer")).thenReturn(new IndexPolicy(
                2, "shadowed_tokenizer", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Mockito.when(policyMgr.getPolicyByName("shadowed")).thenReturn(new IndexPolicy(
                3, "shadowed", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "shadowed_tokenizer")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String legacyFiltered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "", "ik", "__default__", "none", null);
            String namedFiltered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(legacyFiltered, namedFiltered);

            String plainSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart"), "smart", "none", "__default__", "none", null);
            String lowercasedByIk = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "AaA", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(plainSmart, lowercasedByIk);

            String reordered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "_--a", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            String canonical = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-_", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(canonical, reordered);

            String lowerCaseDisabled = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false",
                            "char_filter_type", "char_replace", "char_filter_pattern", "A",
                            "char_filter_replacement", "a"),
                    "", "ik", "__default__", "none", null);
            String lowerCaseDisabledPlain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false"),
                    "", "ik", "__default__", "none", null);
            Assertions.assertNotEquals(lowerCaseDisabledPlain, lowerCaseDisabled);

            String shadowed = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "shadowed", "char_filter_type", "char_replace",
                            "char_filter_pattern", "A", "char_filter_replacement", "a"),
                    "shadowed", "none", "__default__", "none", null);
            String shadowedPlain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "shadowed"), "shadowed", "none", "__default__", "none", null);
            Assertions.assertNotEquals(shadowedPlain, shadowed);
        }
    }

    @Test
    public void testLegacyIkIdentityMatchesEquivalentCustomAnalyzer() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart_analyzer")).thenReturn(new IndexPolicy(
                1, "smart_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("max_word_analyzer")).thenReturn(new IndexPolicy(
                2, "max_word_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_max_word")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String customSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart_analyzer"), "smart_analyzer", "none", "__default__", "none", null);
            String customMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "max_word_analyzer"), "max_word_analyzer", "none",
                    "__default__", "none", null);

            Assertions.assertEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik"), "", "ik", "__default__", "none", null));
            Assertions.assertEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_max_word"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null));
            Assertions.assertNotEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "char_filter_type", "char_replace", "char_filter_pattern", "-"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "lower_case", "false"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik", "char_filter_type", "char_replace", "char_filter_pattern", "-"), "ik", "none",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik", "lower_case", "false"), "ik", "none",
                    "__default__", "none", null));
        }
    }

    @Test
    public void testLegacyIkIdentityIgnoresShadowingTokenizerPolicy() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("ik_smart")).thenReturn(new IndexPolicy(
                1, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Mockito.when(policyMgr.getPolicyByName("smart_analyzer")).thenReturn(new IndexPolicy(
                2, "smart_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String shadowedCustom = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart_analyzer"), "smart_analyzer", "none",
                    "__default__", "none", null);
            String legacy = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik"), "", "ik", "__default__", "none", null);
            Assertions.assertNotEquals(shadowedCustom, legacy);
        }
    }

    @Test
    public void testDisabledLowercaseIkModesHaveDistinctIdentities() {
        String analyzerMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", "ik", "lower_case", "false"), "ik", "none",
                "__default__", "none", null);
        String legacySmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik", "lower_case", "false"), "", "ik",
                "__default__", "none", null);
        String legacyMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik", "parser_mode", "ik_max_word", "lower_case", "false"),
                "", "ik", "__default__", "none", null);
        String lowercaseMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null);
        String lowercaseSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik"), "", "ik", "__default__", "none", null);

        Assertions.assertNotEquals(legacySmart, analyzerMaxWord);
        Assertions.assertEquals(legacyMaxWord, analyzerMaxWord);
        Assertions.assertNotEquals(lowercaseMaxWord, analyzerMaxWord);
        Assertions.assertNotEquals(lowercaseSmart, legacySmart);
    }
}
