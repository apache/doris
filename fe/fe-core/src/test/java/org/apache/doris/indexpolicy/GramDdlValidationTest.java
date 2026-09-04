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

package org.apache.doris.indexpolicy;

import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Task 14：gram 族 analyzer 的图谱约束（R31：ngram tokenizer 带 mode 时禁止叠加任何
 * token filter）与索引属性约束（gram 族只能用于 SNII、不支持短语、禁止索引级 char_filter、
 * support_phrase 未显式指定时默认写为 false）。
 */
public class GramDdlValidationTest {

    private IndexPolicyMgr manager;

    @BeforeEach
    public void setUp() {
        manager = new IndexPolicyMgr();
        // gram_sparse_tok：自定义 TOKENIZER，ngram + mode=sparse（gram 族）。
        manager.replayCreateIndexPolicy(policy(1L, "gram_sparse_tok", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "mode", "sparse")));
        // gram_sparse：只引用 tokenizer、不带任何 token_filter 的合法 gram 族 analyzer。
        manager.replayCreateIndexPolicy(policy(2L, "gram_sparse", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "gram_sparse_tok")));
        // plain_tok / plain：legacy ngram（min_gram/max_gram，无 mode），不属于 gram 族。
        manager.replayCreateIndexPolicy(policy(3L, "plain_tok", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "min_gram", "2", "max_gram", "3")));
        manager.replayCreateIndexPolicy(policy(4L, "plain", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "plain_tok")));
        // gram_dense_tok：ngram + mode=dense，用于验证 R31 对 dense 模式同样生效（比 brief 更严格）。
        manager.replayCreateIndexPolicy(policy(5L, "gram_dense_tok", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "mode", "dense")));
    }

    // ---------------- resolveGramTokenizerMode ----------------

    @Test
    public void testResolveGramMode() {
        Assertions.assertEquals(Optional.of("sparse"), manager.resolveGramTokenizerMode("gram_sparse"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("plain"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("english"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("no_such_analyzer"));
    }

    // ---------------- validateAnalyzerGraphLocked（R31，经 createIndexPolicy 触发） ----------------

    @Test
    public void testLowercaseFilterRejectedWithSparseMode() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("token_filter", "lowercase");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("lowercase token filter cannot be combined"),
                e.getMessage());
    }

    @Test
    public void testOtherFilterRejectedWithSparseMode() {
        // lowercase 以外的任意 token filter（这里用内置的 asciifolding）同样必须被拒绝：
        // R31 要求 gram 族 analyzer 必须是纯 tokenizer，不止针对 lowercase。
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("token_filter", "asciifolding");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer2", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("cannot be combined"), e.getMessage());
    }

    @Test
    public void testLowercaseFilterRejectedWithDenseMode() {
        // 修正裁决 R31 比 brief 更严格：dense 模式也必须拒绝，不能像 brief 草稿那样放过 dense。
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_dense_tok");
        props.put("token_filter", "lowercase");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer3", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("lowercase token filter cannot be combined"),
                e.getMessage());
    }

    @Test
    public void testGramTokenizerOnlyAnalyzerRemainsValid() throws Exception {
        // 纯 tokenizer（不带任何 token_filter）的 gram 族 analyzer 必须继续被允许，
        // 不能被 R31 误伤；"gram_sparse" 本身就是这种合法形态（setUp 中已注册）。
        Assertions.assertFalse(manager.validateAnalyzerUsesCommonGrams("gram_sparse"));
    }

    // ---------------- InvertedIndexUtil.checkInvertedIndexParser 的索引属性约束 ----------------

    @Test
    public void testIndexPropertiesForGramAnalyzer() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        withIndexPolicyManager(mockMgr, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, props,
                        TInvertedIndexFileStorageFormat.SNII)));
        // support_phrase 未显式给出时必须被强制写为 "false"，覆盖 Index 构造函数「存在
        // analyzer 时默认 true」的通用规则。
        Assertions.assertEquals("false", props.get("support_phrase"));
    }

    @Test
    public void testIndexPropertiesRejectExplicitSupportPhraseTrue() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> phrase = new HashMap<>();
        phrase.put("analyzer", "gram_sparse");
        phrase.put("support_phrase", "true");
        withIndexPolicyManager(mockMgr, () -> {
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, phrase,
                            TInvertedIndexFileStorageFormat.SNII));
            Assertions.assertTrue(e.getMessage().contains("does not support phrase"), e.getMessage());
        });
    }

    @Test
    public void testIndexPropertiesRejectNonSniiStorageFormat() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> v2 = new HashMap<>();
        v2.put("analyzer", "gram_sparse");
        withIndexPolicyManager(mockMgr, () -> {
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, v2,
                            TInvertedIndexFileStorageFormat.V2));
            Assertions.assertTrue(e.getMessage().contains("requires inverted_index_storage_format = SNII"),
                    e.getMessage());
        });
    }

    @Test
    public void testIndexPropertiesRejectCharFilterWithGramAnalyzer() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "-");
        withIndexPolicyManager(mockMgr, () -> {
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, props,
                            TInvertedIndexFileStorageFormat.SNII));
            Assertions.assertTrue(e.getMessage().contains("char_filter cannot be used with gram tokenizer"),
                    e.getMessage());
        });
    }

    private static IndexPolicyMgr gramAnalyzerManager() throws Exception {
        IndexPolicyMgr mockMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(mockMgr.resolveGramTokenizerMode("gram_sparse")).thenReturn(Optional.of("sparse"));
        Mockito.when(mockMgr.validateAnalyzerUsesCommonGrams("gram_sparse")).thenReturn(false);
        return mockMgr;
    }

    private static void withIndexPolicyManager(IndexPolicyMgr manager, Runnable action) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            action.run();
        }
    }

    private static IndexPolicy policy(long id, String name, IndexPolicyTypeEnum type, Map<String, String> props) {
        return new IndexPolicy(id, name, type, new HashMap<>(props));
    }
}
