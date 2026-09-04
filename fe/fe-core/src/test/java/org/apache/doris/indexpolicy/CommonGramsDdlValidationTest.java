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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class CommonGramsDdlValidationTest {

    @Test
    public void testAcceptsTerminalCommonGramsGraph() throws Exception {
        IndexPolicyMgr manager = managerWithCommonGrams();
        manager.replayCreateIndexPolicy(policy(3, "lower", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(policy(4, "domain_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group", "token_filter", "lower,domain_grams")));

        Assertions.assertTrue(manager.validateAnalyzerUsesCommonGrams("domain_analyzer"));
    }

    @Test
    public void testRejectsDuplicateAndNonTerminalCommonGrams() {
        IndexPolicyMgr duplicate = managerWithCommonGrams();
        duplicate.replayCreateIndexPolicy(policy(3, "dup", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group",
                        "token_filter", "domain_grams,domain_grams")));
        assertAnalyzerError(duplicate, "dup", "exactly once as the terminal token filter");

        IndexPolicyMgr nonTerminal = managerWithCommonGrams();
        nonTerminal.replayCreateIndexPolicy(policy(3, "lower", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "lowercase")));
        nonTerminal.replayCreateIndexPolicy(policy(4, "non_terminal", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group",
                        "token_filter", "domain_grams,lower")));
        assertAnalyzerError(nonTerminal, "non_terminal",
                "exactly once as the terminal token filter");
    }

    @Test
    public void testRejectsUnsafePositionFactories() {
        IndexPolicyMgr unsafeTokenizer = managerWithCommonGrams();
        unsafeTokenizer.replayCreateIndexPolicy(policy(3, "unsafe_tokenizer",
                IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "standard", "token_filter", "domain_grams")));
        assertAnalyzerError(unsafeTokenizer, "unsafe_tokenizer",
                "tokenizer 'standard' does not guarantee unit position increments");

        IndexPolicyMgr unsafeFilter = managerWithCommonGrams();
        unsafeFilter.replayCreateIndexPolicy(policy(3, "word_parts",
                IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "word_delimiter")));
        unsafeFilter.replayCreateIndexPolicy(policy(4, "unsafe_filter",
                IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group",
                        "token_filter", "word_parts,domain_grams")));
        assertAnalyzerError(unsafeFilter, "unsafe_filter",
                "token filter 'word_parts' does not guarantee unit position increments");

        IndexPolicyMgr stackedAscii = managerWithCommonGrams();
        stackedAscii.replayCreateIndexPolicy(policy(3, "folded",
                IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding", "preserve_original", "true")));
        stackedAscii.replayCreateIndexPolicy(policy(4, "stacked_ascii",
                IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group", "token_filter", "folded,domain_grams")));
        assertAnalyzerError(stackedAscii, "stacked_ascii",
                "token filter 'folded' does not guarantee unit position increments");

        IndexPolicyMgr malformedAscii = managerWithCommonGrams();
        malformedAscii.replayCreateIndexPolicy(policy(3, "malformed_folded",
                IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding", "preserve_original", "garbage")));
        malformedAscii.replayCreateIndexPolicy(policy(4, "malformed_ascii",
                IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "char_group",
                        "token_filter", "malformed_folded,domain_grams")));
        assertAnalyzerError(malformedAscii, "malformed_ascii",
                "token filter 'malformed_folded' does not guarantee unit position increments");
    }

    @Test
    public void testPlainAnalyzerBehaviorRemainsUnchanged() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(policy(1, "plain_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "standard", "token_filter", "lowercase")));

        Assertions.assertFalse(manager.validateAnalyzerUsesCommonGrams("plain_analyzer"));
    }

    @Test
    public void testRejectsCommonGramsPropertiesBeyondType() {
        IndexPolicyMgr manager = new IndexPolicyMgr();

        // The word list common_grams matches against is a BE-local file named by be.conf's
        // common_grams_wordset_path, so no policy property can name one. An unknown property has
        // to fail the DDL rather than be silently ignored, or the user would believe a word list
        // they supplied was in effect.
        for (Map<String, String> properties : List.of(
                Map.of("type", "common_grams", "words", "FILE:db/index_common_words/domain.txt"),
                Map.of("type", "common_grams", "format", "wordset"),
                Map.of("type", "common_grams", "ignore_case", "false"))) {
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> manager.createIndexPolicy(false, "domain_grams",
                            IndexPolicyTypeEnum.TOKEN_FILTER, properties));
            Assertions.assertTrue(
                    exception.getMessage().contains("common_grams token filter does not support"),
                    exception.getMessage());
        }
    }

    private static IndexPolicyMgr managerWithCommonGrams() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(commonGramsPolicy());
        return manager;
    }

    private static IndexPolicy commonGramsPolicy() {
        return policy(2, "domain_grams", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "common_grams"));
    }

    private static IndexPolicy policy(long id, String name, IndexPolicyTypeEnum type,
            Map<String, String> properties) {
        return new IndexPolicy(id, name, type, properties);
    }

    private static void assertAnalyzerError(IndexPolicyMgr manager, String analyzer,
            String expectedMessage) {
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> manager.validateAnalyzerUsesCommonGrams(analyzer));
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage),
                exception.getMessage());
    }
}
