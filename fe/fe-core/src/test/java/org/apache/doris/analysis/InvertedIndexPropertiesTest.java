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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvertedIndexPropertiesTest {

    @Test
    public void testRejectsTypeOnlyAndExplicitDefaultPinyinAnalyzers() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("pinyin_type_only")).thenReturn(new IndexPolicy(
                1, "pinyin_type_only", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "pinyin")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_defaults")).thenReturn(new IndexPolicy(
                2, "pinyin_defaults", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_first_letter", "true",
                        "keep_full_pinyin", "true", "keep_original", "false",
                        "ignore_pinyin_offset", "true", "limit_first_letter_length", "16")));
        Mockito.when(policyMgr.getPolicyByName("type_only_analyzer")).thenReturn(new IndexPolicy(
                3, "type_only_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_type_only")));
        Mockito.when(policyMgr.getPolicyByName("defaulted_analyzer")).thenReturn(new IndexPolicy(
                4, "defaulted_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_defaults")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        IndexDefinition typeOnly = new IndexDefinition("idx_type_only", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "type_only_analyzer"), "");
        IndexDefinition explicitDefaults = new IndexDefinition("idx_explicit_defaults", false,
                List.of("content"), "INVERTED", Map.of("analyzer", "defaulted_analyzer"), "");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                    StringType.INSTANCE, List.of(typeOnly, explicitDefaults)));
        }
    }

    @Test
    public void testCreateTableRejectsEquivalentCanonicalComponentSettings() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "basic_ab", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "ab")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "basic_baba", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "baba")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "basic_ab_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "basic_ab")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "basic_baba_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "basic_baba")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "pinyin_default", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "pinyin_fixed", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "fixed_pinyin_offset", "true")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(7, "pinyin_default_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_default")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(8, "pinyin_fixed_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_fixed")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(9, "icu_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(10, "icu_empty", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(11, "icu_default_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "standard", "char_filter", "icu_default")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(12, "icu_empty_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "standard", "char_filter", "icu_empty")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_basic_ab", "basic_ab_analyzer"),
                                    invertedIndexDefinition("idx_basic_baba", "basic_baba_analyzer")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_pinyin_default", "pinyin_default_analyzer"),
                                    invertedIndexDefinition("idx_pinyin_fixed", "pinyin_fixed_analyzer")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_icu_default", "icu_default_analyzer"),
                                    invertedIndexDefinition("idx_icu_empty", "icu_empty_analyzer")))));
        }
    }

    @Test
    public void testCreateTableRejectsReorderedCollectionAndIneffectiveModeAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        long id = 100;
        String[][] aliases = {
                {"ngram_ld", "TOKENIZER", "type=ngram;token_chars=letter,digit"},
                {"ngram_dll", "TOKENIZER", "type=ngram;token_chars=digit,letter,letter"},
                {"edge_ab", "TOKENIZER", "type=edge_ngram;token_chars=letter,custom;custom_token_chars=ab"},
                {"edge_bba", "TOKENIZER", "type=edge_ngram;token_chars=custom,letter;custom_token_chars=bba"},
                {"group_ab", "TOKENIZER", "type=char_group;tokenize_on_chars=[a],[b]"},
                {"group_bba", "TOKENIZER", "type=char_group;tokenize_on_chars=[b],[a],[b]"},
                {"protect_ab", "TOKEN_FILTER", "type=word_delimiter;protected_words=foo,bar"},
                {"protect_bba", "TOKEN_FILTER", "type=word_delimiter;protected_words=bar,foo,bar"},
                {"types_ab", "TOKEN_FILTER", "type=word_delimiter;type_table=[a => DIGIT],[b => ALPHA]"},
                {"types_bba", "TOKEN_FILTER", "type=word_delimiter;type_table=[b => ALPHA],[a => ALPHA],[a => DIGIT]"},
                {"nfd_default", "CHAR_FILTER", "type=icu_normalizer;name=nfd"},
                {"nfd_decompose", "CHAR_FILTER", "type=icu_normalizer;name=nfd;mode=decompose"}};
        for (String[] alias : aliases) {
            Map<String, String> properties = new HashMap<>();
            for (String entry : alias[2].split(";")) {
                String[] keyValue = entry.split("=", 2);
                properties.put(keyValue[0], keyValue[1]);
            }
            IndexPolicyTypeEnum type = IndexPolicyTypeEnum.valueOf(alias[1]);
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(id++, alias[0], type, properties));
            String componentKey = type == IndexPolicyTypeEnum.TOKENIZER ? "tokenizer"
                    : type == IndexPolicyTypeEnum.TOKEN_FILTER ? "token_filter" : "char_filter";
            Map<String, String> analyzer = new HashMap<>();
            analyzer.put("tokenizer", "standard");
            analyzer.put(componentKey, alias[0]);
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    id++, alias[0] + "_analyzer", IndexPolicyTypeEnum.ANALYZER, analyzer));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (int i = 0; i < aliases.length; i += 2) {
                String left = aliases[i][0];
                String right = aliases[i + 1][0];
                Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                        StringType.INSTANCE, List.of(
                                invertedIndexDefinition("idx_" + left, left + "_analyzer"),
                                invertedIndexDefinition("idx_" + right, right + "_analyzer"))),
                        left + " and " + right + " must share one analyzer identity");
            }
        }
    }

    @Test
    public void testCreateTableRejectsDefaultRestatingAndCoveredComponentAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        long id = 200;
        String[][] aliases = {
                {"py_tk_plain", "TOKENIZER", "type=pinyin"},
                {"py_tk_untrimmed", "TOKENIZER", "type=pinyin;trim_whitespace=false"},
                {"py_tf_joined", "TOKEN_FILTER", "type=pinyin;keep_first_letter=false;keep_full_pinyin=false;"
                        + "keep_none_chinese=false;keep_joined_full_pinyin=true"},
                {"py_tf_joined_dedup", "TOKEN_FILTER", "type=pinyin;keep_first_letter=false;keep_full_pinyin=false;"
                        + "keep_none_chinese=false;keep_joined_full_pinyin=true;remove_duplicated_term=true"},
                {"basic_plain", "TOKENIZER", "type=basic"},
                {"basic_alnum", "TOKENIZER", "type=basic;extra_chars=A0"},
                {"ngram_letter", "TOKENIZER", "type=ngram;token_chars=letter"},
                {"ngram_letter_custom_a", "TOKENIZER", "type=ngram;token_chars=letter,custom;custom_token_chars=A"},
                {"group_letter", "TOKENIZER", "type=char_group;tokenize_on_chars=[letter]"},
                {"group_letter_a", "TOKENIZER", "type=char_group;tokenize_on_chars=[letter],[A]"},
                {"wd_b_digit", "TOKEN_FILTER", "type=word_delimiter;type_table=[b => DIGIT]"},
                {"wd_a_lower_b_digit", "TOKEN_FILTER", "type=word_delimiter;type_table=[a => LOWER],[b => DIGIT]"}};
        for (String[] alias : aliases) {
            Map<String, String> properties = new HashMap<>();
            for (String entry : alias[2].split(";")) {
                String[] keyValue = entry.split("=", 2);
                properties.put(keyValue[0], keyValue[1]);
            }
            IndexPolicyTypeEnum type = IndexPolicyTypeEnum.valueOf(alias[1]);
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(id++, alias[0], type, properties));
            Map<String, String> analyzer = new HashMap<>();
            analyzer.put("tokenizer", type == IndexPolicyTypeEnum.TOKENIZER ? alias[0] : "keyword");
            if (type == IndexPolicyTypeEnum.TOKEN_FILTER) {
                analyzer.put("token_filter", alias[0]);
            }
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    id++, alias[0] + "_analyzer", IndexPolicyTypeEnum.ANALYZER, analyzer));
        }
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(id++, "custom_lowercase", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (int i = 0; i < aliases.length; i += 2) {
                String left = aliases[i][0];
                String right = aliases[i + 1][0];
                Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                        StringType.INSTANCE, List.of(
                                invertedIndexDefinition("idx_" + left, left + "_analyzer"),
                                invertedIndexDefinition("idx_" + right, right + "_analyzer"))),
                        left + " and " + right + " must share one analyzer identity");
            }
            Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                    StringType.INSTANCE, List.of(
                            invertedNormalizerIndexDefinition("idx_builtin_lowercase", "lowercase"),
                            invertedNormalizerIndexDefinition("idx_custom_lowercase", "custom_lowercase"))),
                    "the built-in lowercase normalizer must share the identity of its custom equivalent");
        }
    }

    private static IndexDefinition invertedIndexDefinition(String name, String analyzer) {
        return new IndexDefinition(name, false, List.of("content"), "INVERTED",
                Map.of("analyzer", analyzer), "");
    }

    private static IndexDefinition invertedNormalizerIndexDefinition(String name, String normalizer) {
        return new IndexDefinition(name, false, List.of("content"), "INVERTED",
                Map.of("normalizer", normalizer), "");
    }

    private static IndexDefinition invertedIndexDefinitionWithOuterLowerA(String name, String analyzer) {
        return new IndexDefinition(name, false, List.of("content"), "INVERTED",
                Map.of("analyzer", analyzer, "char_filter_type", "char_replace",
                        "char_filter_pattern", "A", "char_filter_replacement", "a"), "");
    }

    @Test
    public void testCreateTableRejectsIneffectiveKeywordBufferSizeAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "keyword_256", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "keyword", "buffer_size", "256")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "keyword_512", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "keyword", "buffer_size", "512")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "keyword_256_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword_256")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "keyword_512_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword_512")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                    StringType.INSTANCE, List.of(
                            invertedIndexDefinition("idx_keyword_256", "keyword_256_analyzer"),
                            invertedIndexDefinition("idx_keyword_512", "keyword_512_analyzer"))));
        }
    }

    @Test
    public void testCreateTableRejectsCaseFoldCarriedThroughNonInteractingCharReplace() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "x_to_y", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "x", "replacement", "y")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "a_to_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "a", "replacement", "b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "x_then_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "x_to_y,fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "lower_x_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "lower_a,x_to_y,fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(7, "ab_then_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "a_to_b,fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(8, "lower_ab_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "lower_a,a_to_b,fold")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_x_then_fold", "x_then_fold"),
                                    invertedIndexDefinition("idx_lower_x_fold", "lower_x_fold")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_ab_then_fold", "ab_then_fold"),
                                    invertedIndexDefinition("idx_lower_ab_fold", "lower_ab_fold")))));
        }
    }

    @Test
    public void testCreateTableRejectsOuterCaseFoldAbsorbedByCustomLowercaseAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "keyword_lower_1", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "keyword_lower_2", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "keyword_plain_1", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "keyword_plain_2", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinitionWithOuterLowerA("idx_outer_lower", "keyword_lower_1"),
                                    invertedIndexDefinition("idx_plain_lower", "keyword_lower_2")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinitionWithOuterLowerA("idx_outer_plain", "keyword_plain_1"),
                                    invertedIndexDefinition("idx_plain_plain", "keyword_plain_2")))));
        }
    }

    private static IndexDefinition normalizerIndexDefinition(String name, String normalizer) {
        return new IndexDefinition(name, false, List.of("content"), "INVERTED",
                Map.of("normalizer", normalizer), "");
    }

    private static IndexDefinition normalizerIndexDefinitionWithOuterLowerA(String name, String normalizer) {
        return new IndexDefinition(name, false, List.of("content"), "INVERTED",
                Map.of("normalizer", normalizer, "char_filter_type", "char_replace",
                        "char_filter_pattern", "A", "char_filter_replacement", "a"), "");
    }

    @Test
    public void testCreateTableRejectsPinyinSettingsBehindDisabledGates() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "pinyin_tf_plain", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "pinyin_tf_ascii_in_joined",
                IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_none_chinese_in_joined_full_pinyin", "true")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "pinyin_tf_separate", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_none_chinese_together", "false")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "pinyin_tf_separate_untokenized",
                IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_none_chinese_together", "false",
                        "none_chinese_pinyin_tokenize", "false")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "pinyin_tk_plain", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "pinyin_tk_ascii_in_joined",
                IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin", "keep_none_chinese_in_joined_full_pinyin", "true")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(7, "pinyin_tk_separate", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin", "keep_none_chinese_together", "false")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(8, "pinyin_tk_separate_untokenized",
                IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin", "keep_none_chinese_together", "false",
                        "none_chinese_pinyin_tokenize", "false")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(9, "pinyin_tk_buffer_only", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin", "keep_first_letter", "false", "keep_full_pinyin", "false",
                        "none_chinese_pinyin_tokenize", "false")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(10, "pinyin_tk_buffer_only_ascii",
                IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "pinyin", "keep_first_letter", "false", "keep_full_pinyin", "false",
                        "none_chinese_pinyin_tokenize", "false",
                        "keep_none_chinese_in_joined_full_pinyin", "true")));
        long id = 20;
        for (String filter : new String[] {"pinyin_tf_plain", "pinyin_tf_ascii_in_joined",
                "pinyin_tf_separate", "pinyin_tf_separate_untokenized"}) {
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(id++, filter + "_analyzer",
                    IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword", "token_filter", filter)));
        }
        for (String tokenizer : new String[] {"pinyin_tk_plain", "pinyin_tk_ascii_in_joined",
                "pinyin_tk_separate", "pinyin_tk_separate_untokenized", "pinyin_tk_buffer_only",
                "pinyin_tk_buffer_only_ascii"}) {
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(id++, tokenizer + "_analyzer",
                    IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", tokenizer)));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_tf_plain", "pinyin_tf_plain_analyzer"),
                                    invertedIndexDefinition("idx_tf_ascii", "pinyin_tf_ascii_in_joined_analyzer")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_tf_separate", "pinyin_tf_separate_analyzer"),
                                    invertedIndexDefinition("idx_tf_separate_untokenized",
                                            "pinyin_tf_separate_untokenized_analyzer")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_tk_plain", "pinyin_tk_plain_analyzer"),
                                    invertedIndexDefinition("idx_tk_ascii", "pinyin_tk_ascii_in_joined_analyzer")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_tk_separate", "pinyin_tk_separate_analyzer"),
                                    invertedIndexDefinition("idx_tk_separate_untokenized",
                                            "pinyin_tk_separate_untokenized_analyzer")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_tk_buffer_only", "pinyin_tk_buffer_only_analyzer"),
                                    invertedIndexDefinition("idx_tk_buffer_only_ascii",
                                            "pinyin_tk_buffer_only_ascii_analyzer")))));
        }
    }

    @Test
    public void testCreateTableRejectsEmptyUnicodeSetFoldAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "fold_empty", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "fold_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[b]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "fold_empty_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "fold_empty")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "lower_fold_empty", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "lower_a,fold_empty")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "fold_b_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "fold_b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(7, "lower_fold_b", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "lower_a,fold_b")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_fold_empty", "fold_empty_only"),
                                    invertedIndexDefinition("idx_lower_fold_empty", "lower_fold_empty")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinition("idx_fold_b", "fold_b_only"),
                                    invertedIndexDefinition("idx_lower_fold_b", "lower_fold_b")))));
        }
    }

    @Test
    public void testCreateTableRejectsOuterCaseFoldAbsorbedByNormalizerAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "norm_lower_1", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "norm_lower_2", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "norm_lower_a_then_lowercase",
                IndexPolicyTypeEnum.NORMALIZER, Map.of("char_filter", "lower_a", "token_filter", "lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "norm_ascii_1", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "asciifolding")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "norm_ascii_2", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "asciifolding")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    normalizerIndexDefinitionWithOuterLowerA("idx_outer_norm_lower", "norm_lower_1"),
                                    normalizerIndexDefinition("idx_norm_lower", "norm_lower_2")))),
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    normalizerIndexDefinition("idx_norm_lower_a", "norm_lower_a_then_lowercase"),
                                    normalizerIndexDefinition("idx_norm_lower", "norm_lower_2")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    normalizerIndexDefinitionWithOuterLowerA("idx_outer_norm_ascii", "norm_ascii_1"),
                                    normalizerIndexDefinition("idx_norm_ascii", "norm_ascii_2")))));
        }
    }

    @Test
    public void testCreateTableRejectsOuterCaseFoldThroughAsciiTransparentFilters() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(1, "ascii", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(2, "wd", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(3, "ascii_lower_1", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "ascii,lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(4, "ascii_lower_2", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "ascii,lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(5, "wd_lower_1", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "wd,lowercase")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(6, "wd_lower_2", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "wd,lowercase")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinitionWithOuterLowerA("idx_outer_ascii_lower", "ascii_lower_1"),
                                    invertedIndexDefinition("idx_ascii_lower", "ascii_lower_2")))),
                    () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                            StringType.INSTANCE, List.of(
                                    invertedIndexDefinitionWithOuterLowerA("idx_outer_wd_lower", "wd_lower_1"),
                                    invertedIndexDefinition("idx_wd_lower", "wd_lower_2")))));
        }
    }

    @Test
    public void testRejectsAmbiguousOuterCharFiltersForSameAnalyzer() {
        IndexDefinition replaceA = new IndexDefinition("idx_replace_a", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "standard", "char_filter_type", "char_replace",
                        "char_filter_pattern", "a", "char_filter_replacement", "b"), "");
        IndexDefinition replaceX = new IndexDefinition("idx_replace_x", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "standard", "char_filter_type", "char_replace",
                        "char_filter_pattern", "x", "char_filter_replacement", "y"), "");

        Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                StringType.INSTANCE, List.of(replaceA, replaceX)));

        IndexDefinition defaultA = new IndexDefinition("idx_default_a", false, List.of("content"),
                "INVERTED", Map.of("char_filter_type", "char_replace", "char_filter_pattern", "a",
                        "char_filter_replacement", "b"), "");
        IndexDefinition defaultX = new IndexDefinition("idx_default_x", false, List.of("content"),
                "INVERTED", Map.of("char_filter_type", "char_replace", "char_filter_pattern", "x",
                        "char_filter_replacement", "y"), "");
        Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                StringType.INSTANCE, List.of(defaultA, defaultX)));
    }

    @Test
    public void testExplicitBuiltinIkSelectsMatchingModeAndLowercase() {
        Column column = new Column("content", PrimitiveType.STRING);
        Index smartNoLowercase = new Index(10, "idx_smart_no_lowercase", List.of("content"),
                IndexType.INVERTED, Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false"), "");
        Index smart = new Index(11, "idx_smart", List.of("content"), IndexType.INVERTED,
                Map.of("parser", "ik", "parser_mode", "ik_smart"), "");
        Index maxWordNoLowercase = new Index(12, "idx_max_word_no_lowercase", List.of("content"),
                IndexType.INVERTED, Map.of("parser", "ik", "parser_mode", "ik_max_word", "lower_case", "false"), "");
        Index maxWord = new Index(13, "idx_max_word", List.of("content"), IndexType.INVERTED,
                Map.of("parser", "ik", "parser_mode", "ik_max_word"), "");
        OlapTable table = new OlapTable();
        table.setIndexes(List.of(smartNoLowercase, smart, maxWordNoLowercase, maxWord));

        Index selected = table.getInvertedIndex(column, List.of(), "ik");
        Assertions.assertSame(maxWord, selected);
        MatchPredicate predicate = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                new StringLiteral("清华大学"), new StringLiteral("清华"), Type.BOOLEAN,
                NullableMode.DEPEND_ON_ARGUMENT, selected, false, "ik");
        TExprNode node = new TExprNode();
        ExprToThriftVisitor.INSTANCE.visitMatchPredicate(predicate, node);
        Assertions.assertEquals("ik", node.getMatchPredicate().getAnalyzerName());
        Assertions.assertEquals("ik_max_word", node.getMatchPredicate().getParserMode());
        Assertions.assertTrue(node.getMatchPredicate().isParserLowercase());
        Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "ik"));
        Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                Map.of("analyzer", "ik", "lower_case", "false"), "ik"));
    }

    @Test
    public void testMatchSelectionPreservesExactAnalyzerSpelling() {
        MatchAny match = new MatchAny(new VarcharLiteral("abc def"), new VarcharLiteral("abc def"), " IK ");
        Assertions.assertAll(
                () -> Assertions.assertEquals("IK", match.getAnalyzer().orElseThrow()),
                () -> Assertions.assertEquals("IK",
                        match.withChildren(match.children()).getAnalyzer().orElseThrow()),
                () -> Assertions.assertEquals("IK",
                        AnalyzerSelector.select(Map.of("analyzer", "IK"), null).analyzer()),
                () -> Assertions.assertEquals("IK",
                        AnalyzerSelector.select(Map.of("analyzer", "IK"), "IK").analyzer()));
    }

    @Test
    public void testAnalyzerMatchingKeepsReplayedCaseDistinctPoliciesSeparate() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                30, "Legacy", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                31, "legacy", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "standard")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "Legacy"), "Legacy"));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "legacy"), "legacy"));
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                            Map.of("analyzer", "Legacy"), "legacy")),
                    () -> Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                            Map.of("analyzer", "legacy"), "Legacy")));
        }
    }

    @Test
    public void testMatchThriftKeepsImplicitAndExplicitLegacyIkBindings() {
        Index index = new Index(1, "idx_legacy_ik", List.of("content"),
                IndexType.INVERTED, Map.of("analyzer", "IK"), "");
        for (String analyzer : List.of("", "IK")) {
            MatchPredicate predicate = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                    new StringLiteral("abc def"), new StringLiteral("abc def"), Type.BOOLEAN,
                    NullableMode.DEPEND_ON_ARGUMENT, index, false, analyzer);
            TExprNode node = new TExprNode();
            ExprToThriftVisitor.INSTANCE.visitMatchPredicate(predicate, node);
            Assertions.assertEquals("IK", node.getMatchPredicate().getAnalyzerName());
        }
    }

    @Test
    public void testAnalyzerResolutionKeepsCanonicalBuiltinsAndExactLegacyBindings() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                40, "IK", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals("IK", InvertedIndexUtil.resolveAnalyzerName(" IK "));
            Assertions.assertEquals("ik", InvertedIndexUtil.resolveAnalyzerName("ik"));
            Assertions.assertEquals("standard", InvertedIndexUtil.resolveAnalyzerName(" StAnDaRd "));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "IK"), "IK"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "IK"), "ik"));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "ik"), "ik"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "ik"), "IK"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "ik"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "IK"));
        }
    }

    @Test
    public void testMixedCaseBuiltinSpellingsStoreBuiltinDespiteNormalizedLegacyPolicies() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                50, "LOWERCASE", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                51, "IK", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword")));
        Map<String, String> mixedNormalizer = new HashMap<>(Map.of("normalizer", "LowerCase"));
        Map<String, String> mixedAnalyzer = new HashMap<>(Map.of("analyzer", "Ik"));
        Map<String, String> exactNormalizer = new HashMap<>(Map.of("normalizer", "LOWERCASE"));

        withIndexPolicyManager(policyMgr, () -> {
            for (Map<String, String> properties : List.of(mixedNormalizer, mixedAnalyzer, exactNormalizer)) {
                Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkInvertedIndexParser("c",
                        PrimitiveType.VARCHAR, properties, TInvertedIndexFileStorageFormat.V3));
            }
            Assertions.assertAll(
                    () -> Assertions.assertEquals("lowercase", mixedNormalizer.get("normalizer")),
                    () -> Assertions.assertEquals("ik", mixedAnalyzer.get("analyzer")),
                    () -> Assertions.assertEquals("LOWERCASE", exactNormalizer.get("normalizer")),
                    () -> Assertions.assertEquals("lowercase", InvertedIndexUtil.resolveAnalyzerName("LowerCase")),
                    () -> Assertions.assertEquals("ik", InvertedIndexUtil.resolveAnalyzerName("Ik")),
                    () -> Assertions.assertEquals("LOWERCASE", InvertedIndexUtil.resolveAnalyzerName("LOWERCASE")));
        });
    }

    @Test
    public void testCreateTableUsesExactLegacyLowercaseNormalizerIdentity() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                60, "lowercase", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                61, "norm_ascii", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                62, "norm_lower", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "lowercase")));

        withIndexPolicyManager(policyMgr, () -> Assertions.assertAll(
                () -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                        StringType.INSTANCE, List.of(
                                normalizerIndexDefinition("idx_legacy_lowercase", "lowercase"),
                                normalizerIndexDefinition("idx_ascii", "norm_ascii")))),
                () -> Assertions.assertTrue(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                        StringType.INSTANCE, List.of(
                                normalizerIndexDefinition("idx_legacy_lowercase", "lowercase"),
                                normalizerIndexDefinition("idx_lower", "norm_lower"))))));
    }

    @Test
    public void testCreateTableRejectsRedundantTokenCharAndReverseCaseAliases() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(70, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(71, "upper_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "a", "replacement", "A")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(72, "ngram_letter", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "letter")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(73, "ngram_letter_a", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "letter,custom", "custom_token_chars", "A")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(74, "group_letter", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "char_group", "tokenize_on_chars", "[letter]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(75, "group_letter_a", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "char_group", "tokenize_on_chars", "[letter],[A]")));
        String[][] analyzers = {
                {"ngram_plain", "ngram_letter", "lower_a"},
                {"ngram_custom_a", "ngram_letter_a", "lower_a"},
                {"group_plain", "group_letter", "lower_a"},
                {"group_literal_a", "group_letter_a", "lower_a"},
                {"keyword_lower", "keyword", null},
                {"upper_keyword_lower", "keyword", "upper_a"}};
        long id = 80;
        for (String[] analyzer : analyzers) {
            Map<String, String> properties = new HashMap<>(
                    Map.of("tokenizer", analyzer[1], "token_filter", "lowercase"));
            if (analyzer[2] != null) {
                properties.put("char_filter", analyzer[2]);
            }
            policyMgr.replayCreateIndexPolicy(
                    new IndexPolicy(id++, analyzer[0], IndexPolicyTypeEnum.ANALYZER, properties));
        }

        List<Executable> checks = new ArrayList<>();
        for (int i = 0; i < analyzers.length; i += 2) {
            String left = analyzers[i][0];
            String right = analyzers[i + 1][0];
            checks.add(() -> Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                    StringType.INSTANCE, List.of(
                            invertedIndexDefinition("idx_" + left, left),
                            invertedIndexDefinition("idx_" + right, right))),
                    left + " and " + right + " must share one analyzer identity"));
        }
        withIndexPolicyManager(policyMgr, () -> Assertions.assertAll(checks));
    }

    private static void assertCheckCharFilterPropertiesThrows(Map<String, String> props, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> InvertedIndexUtil.checkCharFilterProperties(props));
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }

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

    @Test
    public void testGetParserModeDefaultForKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        Assertions.assertEquals("search",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeExplicitForKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        props.put("parser_mode", "extended");
        Assertions.assertEquals("extended",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForAnalyzerKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "kuromoji");
        Assertions.assertEquals("search",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeExplicitForAnalyzerKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "kuromoji");
        props.put("parser_mode", "normal");
        Assertions.assertEquals("normal",
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

    @Test
    public void testCheckCharFilterPropertiesRejectsEmptyReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must be a single non-empty character");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsMultiCharReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "xyz");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must be a single non-empty character");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsMissingType() {
        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(new HashMap<>()));
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsInvalidType() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "invalid");
        assertCheckCharFilterPropertiesThrows(props, "Invalid 'char_filter_type'");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsMissingPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        assertCheckCharFilterPropertiesThrows(props, "Missing 'char_filter_pattern' for 'char_replace' filter type");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsEmptyPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "");
        assertCheckCharFilterPropertiesThrows(props, "Missing 'char_filter_pattern' for 'char_replace' filter type");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsNonAsciiPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "中");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_pattern' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsLatin1Pattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "é");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_pattern' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsNullReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(props));
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsNonAsciiReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "中");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsLatin1Replacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "é");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsSingleAsciiReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "_");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(props));
    }

    @Test
    public void testCheckInvertedIndexParserAllowsDotCharFilterPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "english");
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "_");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkInvertedIndexParser("c",
                PrimitiveType.VARCHAR, props, TInvertedIndexFileStorageFormat.V2));
    }

    @Test
    public void testPlainCustomAnalyzerBehaviorRemainsUnchanged() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);

        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARIANT,
                        new HashMap<>(Map.of("analyzer", "plain_analyzer")),
                        TInvertedIndexFileStorageFormat.V3)));
    }

    @Test
    public void testResolvedCustomPolicyKeepsExactLegacyNameInIndexProperties() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        IndexPolicy exactPolicy = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "standard"));
        Mockito.when(manager.getPolicyByName("IK_SMART")).thenReturn(exactPolicy);

        Map<String, String> properties = new HashMap<>(Map.of("analyzer", " IK_SMART "));
        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, properties,
                        TInvertedIndexFileStorageFormat.V3)));

        Assertions.assertEquals("IK_SMART", properties.get("analyzer"));

        Map<String, String> normalizerProperties = new HashMap<>(Map.of("normalizer", " IK_SMART "));
        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR,
                        normalizerProperties, TInvertedIndexFileStorageFormat.V3)));
        Assertions.assertEquals("IK_SMART", normalizerProperties.get("normalizer"));
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

    private static void withIndexPolicyManager(IndexPolicyMgr manager, Runnable action) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            action.run();
        }
    }

    // --- norms ---

    @Test
    public void testNormsPropertyAccepted() throws AnalysisException {
        for (String value : new String[] {"true", "false"}) {
            Map<String, String> props = new HashMap<>();
            props.put("parser", "english");
            props.put("norms", value);
            InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.STRING, props,
                    TInvertedIndexFileStorageFormat.V2);
        }
    }

    @Test
    public void testNormsPropertyRejectsOtherValues() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "english");
        props.put("norms", "yes");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.STRING, props,
                        TInvertedIndexFileStorageFormat.V2));
        Assertions.assertTrue(exception.getMessage().contains("norms must be true or false"),
                exception.getMessage());
    }

    // The SNII gate in checkInvertedIndexParser sees the parent VARIANT type on a whole-column
    // index and the sub-column type on a field_pattern index. Only the latter can be judged,
    // so VARIANT itself must pass.
    @Test
    public void testSniiAcceptsVariantColumn() throws AnalysisException {
        InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.VARIANT,
                new HashMap<String, String>(), TInvertedIndexFileStorageFormat.SNII);
    }

}
