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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Task 14: the analyzer graph constraint for gram-family analyzers (R31: an ngram tokenizer with
 * mode may not stack any token filter) and the index property constraints (a gram-family index
 * only works on SNII, does not support phrases, forbids an index-level char_filter, and defaults
 * support_phrase to false when it is not given explicitly).
 */
public class GramDdlValidationTest {

    private IndexPolicyMgr manager;

    @BeforeEach
    public void setUp() {
        manager = new IndexPolicyMgr();
        // gram_sparse_tok: a custom TOKENIZER, ngram + mode=sparse (gram family).
        manager.replayCreateIndexPolicy(policy(1L, "gram_sparse_tok", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "mode", "sparse")));
        // gram_sparse: a valid gram-family analyzer referencing only the tokenizer, no token_filter.
        manager.replayCreateIndexPolicy(policy(2L, "gram_sparse", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "gram_sparse_tok")));
        // plain_tok / plain: legacy ngram (min_gram/max_gram, no mode), not gram family.
        manager.replayCreateIndexPolicy(policy(3L, "plain_tok", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "min_gram", "2", "max_gram", "3")));
        manager.replayCreateIndexPolicy(policy(4L, "plain", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "plain_tok")));
        // gram_dense_tok: ngram + mode=dense, to check that R31 applies to dense as well
        // (stricter than the brief).
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

    // ------------- validateAnalyzerGraphLocked (R31, triggered via createIndexPolicy) -------------

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
        // Any token filter other than lowercase (the built-in asciifolding here) must be rejected
        // too: R31 requires a gram-family analyzer to be a bare tokenizer, not just free of
        // lowercase.
        Map<String, String> props = new HashMap<>();
        props.put("tokenizer", "gram_sparse_tok");
        props.put("token_filter", "asciifolding");
        UserException e = Assertions.assertThrows(UserException.class,
                () -> manager.createIndexPolicy(false, "bad_analyzer2", IndexPolicyTypeEnum.ANALYZER, props));
        Assertions.assertTrue(e.getMessage().contains("cannot be combined"), e.getMessage());
    }

    @Test
    public void testLowercaseFilterRejectedWhenNotFirstInChain() {
        // When lowercase is not first in the chain, the message must still be the more specific
        // lowercase hint: what the user has to change is "switch to the tokenizer's own
        // lower_case", not remove asciifolding first.
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
        // The corrected ruling R31 is stricter than the brief: dense mode must be rejected too,
        // rather than let through as in the brief's draft.
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
        // A gram-family analyzer that is a bare tokenizer (no token_filter at all) must stay
        // allowed and must not be caught by R31; "gram_sparse" is exactly that valid shape
        // (registered in setUp).
        Assertions.assertFalse(manager.validateAnalyzerUsesCommonGrams("gram_sparse"));
    }

    // ---------- index property constraints of InvertedIndexUtil.checkInvertedIndexParser ----------

    @Test
    public void testIndexPropertiesForGramAnalyzer() throws Exception {
        IndexPolicyMgr mockMgr = gramAnalyzerManager();
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "gram_sparse");
        withIndexPolicyManager(mockMgr, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, props,
                        TInvertedIndexFileStorageFormat.SNII)));
        // When support_phrase is not given explicitly it must be forced to "false", overriding the
        // general rule of the Index constructor that "an analyzer implies true".
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

    /**
     * The ADD INDEX / standalone CREATE INDEX path: {@code CreateIndexOp#validate} materializes the
     * Index first, and {@code SchemaChangeHandler#processAddIndex} only runs {@code checkColumn}
     * afterwards. The gram family's support_phrase=false default is written into the
     * IndexDefinition during checkColumn and has to be copied back by
     * {@code IndexDefinition#applyPropertiesTo} for the persisted Index to carry it.
     *
     * <p>{@code processAddIndex} needs a complete Env/OlapTable to run, so this drives the same
     * three steps in its call order (translateToCatalogStyle -> checkColumn -> applyPropertiesTo).
     */
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
            // Materialization happens before checkColumn, so the Index constructor has filled in
            // true by the rule "an analyzer implies phrase support"
            Assertions.assertEquals("true", index.getProperties().get("support_phrase"));

            // checkColumn inside processAddIndex: the gram-family default lands in the
            // IndexDefinition only
            indexDef.checkColumn(new Column("msg", PrimitiveType.STRING), KeysType.DUP_KEYS, false,
                    TInvertedIndexFileStorageFormat.SNII);
            Assertions.assertEquals("false", indexDef.getProperties().get("support_phrase"));

            // Only after the write-back does the Index that is actually persisted carry
            // support_phrase=false
            indexDef.applyPropertiesTo(index);
            Assertions.assertEquals("false", index.getProperties().get("support_phrase"));
            // The write-back must modify the same instance in place: processAddIndex's caller
            // already holds this reference
            Assertions.assertSame(index, createIndexOp.getIndex());
        });
    }

    /**
     * The write-back only overwrites keys present in the IndexDefinition: a default filled in by
     * the Index constructor's general rules that the IndexDefinition side lacks (such as
     * lower_case=true for a parser index) must be preserved as is.
     */
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
