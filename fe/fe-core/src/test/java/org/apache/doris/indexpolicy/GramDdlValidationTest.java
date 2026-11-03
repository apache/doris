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

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Tests gram policy and index validation. */
public class GramDdlValidationTest {

    private IndexPolicyMgr manager;

    @BeforeEach
    public void setUp() {
        manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(policy(1L, "gram_sparse_tok", IndexPolicyTypeEnum.TOKENIZER,
                ImmutableMap.of("type", "ngram", "mode", "sparse")));
        manager.replayCreateIndexPolicy(policy(2L, "gram_sparse", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "gram_sparse_tok")));
        manager.replayCreateIndexPolicy(policy(3L, "plain_tok", IndexPolicyTypeEnum.TOKENIZER,
                ImmutableMap.of("type", "ngram", "min_gram", "2", "max_gram", "3")));
        manager.replayCreateIndexPolicy(policy(4L, "plain", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "plain_tok")));
        manager.replayCreateIndexPolicy(policy(5L, "gram_dense_tok", IndexPolicyTypeEnum.TOKENIZER,
                ImmutableMap.of("type", "ngram", "mode", "dense")));
    }

    @Test
    public void testResolveGramMode() {
        Assertions.assertEquals(Optional.of("sparse"), manager.resolveGramTokenizerMode("gram_sparse"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("plain"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("english"));
        Assertions.assertEquals(Optional.empty(), manager.resolveGramTokenizerMode("no_such_analyzer"));
    }

    @Test
    public void testOmittedMaxGramIsValidatedAgainstTheDefaultBackendApplies() {
        Map<String, String> tooLong = new HashMap<>();
        tooLong.put("type", "ngram");
        tooLong.put("mode", "sparse");
        tooLong.put("min_gram", "5");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "omitted_max_tok",
                        IndexPolicyTypeEnum.TOKENIZER, tooLong));
        Assertions.assertTrue(e.getMessage().contains("min_gram (5) must be <= max_gram (4)"),
                e.getMessage());

        Map<String, String> spelled = new HashMap<>();
        spelled.put("type", "ngram");
        spelled.put("mode", "sparse");
        spelled.put("min_gram", "5");
        spelled.put("max_gram", "8");
        expectValidationAccepts("spelled_max_tok", spelled);

        Map<String, String> withinDefault = new HashMap<>();
        withinDefault.put("type", "ngram");
        withinDefault.put("mode", "sparse");
        withinDefault.put("min_gram", "3");
        expectValidationAccepts("within_default_tok", withinDefault);
    }

    private void expectValidationAccepts(String name, Map<String, String> props) {
        try {
            manager.createIndexPolicy(false, name, IndexPolicyTypeEnum.TOKENIZER, props);
        } catch (UserException e) {
            Assertions.fail("validation rejected " + name + ": " + e.getMessage());
        } catch (RuntimeException expectedWithoutEditLog) {
            // Reaching persistence shows validation succeeded.
        }
    }

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
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("token_filter", "asciifolding");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer2", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("cannot be combined"), e.getMessage());
    }

    @Test
    public void testLowercaseFilterRejectedWhenNotFirstInChain() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("token_filter", "asciifolding,lowercase");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer4", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("lowercase token filter cannot be combined"),
                e.getMessage());
    }

    @Test
    public void testLowercaseFilterRejectedWithDenseMode() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_dense_tok");
        props.put("token_filter", "lowercase");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer3", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("lowercase token filter cannot be combined"),
                e.getMessage());
    }

    @Test
    public void testCharFilterRejectedWithSparseMode() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("char_filter", "char_replace");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer_cf", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("cannot be combined"), e.getMessage());
    }

    @Test
    public void testCharFilterRejectedWithDenseMode() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_dense_tok");
        props.put("char_filter", "char_replace");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer_cf2", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("cannot be combined"), e.getMessage());
    }

    @Test
    public void testCharFilterStillAllowedOnNonGramAnalyzer() {
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "plain_tok");
        props.put("char_filter", "char_replace");
        try {
            manager.createIndexPolicy(false, "plain_with_cf", IndexPolicyTypeEnum.ANALYZER, props);
        } catch (UserException e) {
            Assertions.fail("a non-gram analyzer must keep its char filter, but validation rejected it: "
                    + e.getMessage());
        } catch (RuntimeException expectedWithoutEditLog) {
            // Reaching persistence shows validation succeeded.
        }
    }

    @Test
    public void testGramTokenizerOnlyAnalyzerRemainsValid() throws Exception {
        Assertions.assertDoesNotThrow(() -> manager.validateAnalyzerExists("gram_sparse"));
        Assertions.assertEquals(Optional.of("sparse"), manager.resolveGramTokenizerMode("gram_sparse"));
    }

    @Test
    public void testIndexPropertiesForGramAnalyzer() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        withIndexPolicyManager(mockMgr, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, props,
                        TInvertedIndexFileStorageFormat.SNII)));
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

    @Test
    public void testAddIndexPathKeepsGramSupportPhraseDefault() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        IndexDefinition indexDef = new IndexDefinition("idx_g", false, Lists.newArrayList("msg"),
                "INVERTED", props, "");
        CreateIndexOp createIndexOp = new CreateIndexOp(null, indexDef, true);
        withIndexPolicyManager(mockMgr, () -> {
            Assertions.assertDoesNotThrow(() -> createIndexOp.validate(null));
            Index index = createIndexOp.getIndex();
            Assertions.assertEquals("true", index.getProperties().get("support_phrase"));

            indexDef.checkColumn(new ColumnDefinition("msg", StringType.INSTANCE, true), KeysType.DUP_KEYS, false,
                    TInvertedIndexFileStorageFormat.SNII);
            Assertions.assertEquals("false", indexDef.getProperties().get("support_phrase"));

            indexDef.applyPropertiesTo(index);
            Assertions.assertEquals("false", index.getProperties().get("support_phrase"));
            Assertions.assertSame(index, createIndexOp.getIndex());
        });
    }

    @Test
    public void testApplyPropertiesToKeepsIndexOnlyDefaults() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "english");
        IndexDefinition indexDef = new IndexDefinition("idx_p", false, Lists.newArrayList("msg"),
                "INVERTED", props, "");
        Index index = new Index(1L, "idx_p", Lists.newArrayList("msg"),
                IndexType.INVERTED, props, "");
        Assertions.assertEquals("true", index.getProperties().get("lower_case"));
        Assertions.assertEquals("true", index.getProperties().get("support_phrase"));

        indexDef.applyPropertiesTo(index);
        Assertions.assertEquals("english", index.getProperties().get("parser"));
        Assertions.assertEquals("true", index.getProperties().get("lower_case"));
        Assertions.assertEquals("true", index.getProperties().get("support_phrase"));
    }

    @Test
    public void testLegacyAddIndexPathKeepsGramSupportPhraseDefault() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        IndexDef indexDef = new IndexDef("idx_g", false, Lists.newArrayList("msg"),
                IndexType.INVERTED, props, "");
        Index index = new Index(1L, "idx_g", Lists.newArrayList("msg"), IndexType.INVERTED, props, "");
        Assertions.assertEquals("true", index.getProperties().get("support_phrase"));

        withIndexPolicyManager(mockMgr, () -> Assertions.assertDoesNotThrow(() ->
                indexDef.checkColumn(new Column("msg", Type.STRING, true), KeysType.DUP_KEYS, false,
                        TInvertedIndexFileStorageFormat.SNII)));
        Assertions.assertEquals("false", indexDef.getProperties().get("support_phrase"));
        indexDef.applyPropertiesTo(index);
        Assertions.assertEquals("false", index.getProperties().get("support_phrase"));
    }

    private static IndexPolicyMgr gramAnalyzerManager() throws Exception {
        IndexPolicyMgr mockMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(mockMgr.resolveGramTokenizerMode("gram_sparse")).thenReturn(Optional.of("sparse"));
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
