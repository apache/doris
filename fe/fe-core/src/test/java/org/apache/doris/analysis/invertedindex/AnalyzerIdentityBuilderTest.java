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
    public void testBuiltinTokenizerIdentityIsCanonicalized() throws Exception {
        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        Assertions.assertEquals("ik_smart",
                resolve.invoke(null, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
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
            Assertions.assertNotEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "char_filter_type", "char_replace"), "", "ik",
                    "__default__", "none", null));
        }
    }
}
