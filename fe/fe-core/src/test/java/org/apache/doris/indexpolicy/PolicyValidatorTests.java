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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
// import org.junit.jupiter.params.ParameterizedTest;
// import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PolicyValidatorTests {

    // AsciiFoldingTokenFilterValidator Tests
    // @Test
    // public void testAsciiFoldingValidator_ValidProperties() throws Exception {
    //     AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();
    //     Map<String, String> props = new HashMap<>();
    //     props.put("preserve_original", "true");
    //     validator.validate(props); // Should not throw
    // }

    @Test
    public void testAsciiFoldingValidator_InvalidProperty() {
        AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("invalid_prop", "value");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("does not support parameter"));
    }

    @Test
    public void testAsciiFoldingValidatorAcceptsPreserveOriginal() throws Exception {
        AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();
        validator.validate(Map.of("type", "asciifolding", "preserve_original", "true"));
        validator.validate(Map.of("type", "asciifolding", "preserve_original", "false"));
    }

    private static IndexPolicy roundTrip(IndexPolicy policy) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        policy.write(new DataOutputStream(bytes));
        return IndexPolicy.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private static IndexPolicyMgr roundTrip(IndexPolicyMgr manager) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        manager.write(new DataOutputStream(bytes));
        return IndexPolicyMgr.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    // @ParameterizedTest
    // @ValueSource(strings = {"yes", "no", "1", "0"})
    // public void testAsciiFoldingValidator_InvalidBooleanValue(String value) {
    //     AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();
    //     Map<String, String> props = new HashMap<>();
    //     props.put("preserve_original", value);

    //     Exception exception = Assertions.assertThrows(DdlException.class,
    //             () -> validator.validate(props));
    //     Assertions.assertTrue(exception.getMessage().contains("must be a boolean value"));
    // }

    // EdgeNGramTokenizerValidator Tests
    @Test
    public void testEdgeNGramValidator_ValidProperties() throws Exception {
        EdgeNGramTokenizerValidator validator = new EdgeNGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "2");
        props.put("max_gram", "5");
        props.put("token_chars", "letter,digit");
        validator.validate(props); // Should not throw
    }

    @Test
    public void testEdgeNGramValidator_MaxLessThanMin() {
        EdgeNGramTokenizerValidator validator = new EdgeNGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "3");
        props.put("max_gram", "2");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("cannot be smaller than min_gram"));
    }

    @Test
    public void testEdgeNGramValidator_InvalidTokenChars() {
        EdgeNGramTokenizerValidator validator = new EdgeNGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("token_chars", "letter,invalid");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("Invalid token_chars value"));
    }

    @Test
    public void testEdgeNGramValidator_CustomTokenCharsWithoutCustom() {
        EdgeNGramTokenizerValidator validator = new EdgeNGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("custom_token_chars", "_-");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("includes 'custom'"));
    }

    // NGramTokenizerValidator Tests (similar to EdgeNGram)
    @Test
    public void testNGramValidator_ValidProperties() throws Exception {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "3");
        props.put("max_gram", "5");
        props.put("max_ngram_diff", "2");
        validator.validate(props); // Should not throw
    }

    @Test
    public void testNGramValidator_DefaultDifferenceLimit() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "1");
        props.put("max_gram", "8");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("less than or equal to: [ 1 ]"));
    }

    @Test
    public void testNGramValidator_ConfiguredDifferenceLimit() throws Exception {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "1");
        props.put("max_gram", "8");
        props.put("max_ngram_diff", "7");
        validator.validate(props); // Should not throw
    }

    @Test
    public void testNGramValidator_InvalidDifferenceLimit() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_ngram_diff", "-1");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("greater than or equal to 0"));
    }

    @Test
    public void testNGramValidator_RejectsNonAsciiDifferenceLimit() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_ngram_diff", "٧");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("non-negative integer"));
    }

    @Test
    public void testNGramValidator_RejectsExcessiveDifferenceLimit() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_ngram_diff", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_DIFF + 1));

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("less than or equal to 255"));
    }

    @Test
    public void testNGramValidator_AcceptsDifferenceLimitBoundary() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", "1");
        props.put("max_gram", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_DIFF + 1));
        props.put("max_ngram_diff", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_DIFF));

        Assertions.assertDoesNotThrow(() -> validator.validate(props));
    }

    @Test
    public void testNGramValidator_AcceptsAbsoluteSizeBoundary() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_SIZE));
        props.put("max_gram", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_SIZE));

        Assertions.assertDoesNotThrow(() -> validator.validate(props));
    }

    @Test
    public void testNGramValidator_RejectsExcessiveAbsoluteSize() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("min_gram", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_SIZE));
        props.put("max_gram", Integer.toString(NGramTokenizerValidator.MAX_NGRAM_SIZE + 1));

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("less than or equal to 1024"));
    }

    @Test
    public void testLegacyNGramPolicyAboveCurrentLimitRemainsValidAfterReplay() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(IndexPolicy.PROP_TYPE, "ngram");
        props.put("min_gram", "2048");
        props.put("max_gram", "2048");

        IndexPolicy replayed = roundTrip(new IndexPolicy(
                1, "legacy_large_ngram", IndexPolicyTypeEnum.TOKENIZER, props));

        Assertions.assertFalse(replayed.isInvalid());

        props.put("max_ngram_diff", "1");
        IndexPolicy current = roundTrip(new IndexPolicy(
                2, "current_large_ngram", IndexPolicyTypeEnum.TOKENIZER, props));
        Assertions.assertTrue(current.isInvalid());
    }

    @Test
    public void testNewNGramPolicyPersistsCompatibilityMarker() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getNextId()).thenReturn(2L);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Map<String, String> props = new HashMap<>();
        props.put(IndexPolicy.PROP_TYPE, "ngram");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            policyMgr.createIndexPolicy(false, "new_ngram", IndexPolicyTypeEnum.TOKENIZER, props);
        }

        Assertions.assertEquals("1",
                policyMgr.getPolicyByName("new_ngram").getProperties().get("max_ngram_diff"));
    }

    @Test
    public void testIkTokenizersAreBuiltIn() {
        Assertions.assertTrue(IndexPolicy.BUILTIN_TOKENIZERS.contains("ik_smart"));
        Assertions.assertTrue(IndexPolicy.BUILTIN_TOKENIZERS.contains("ik_max_word"));
    }

    @Test
    public void testExactLegacyPolicyPrecedesBuiltinValidation() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                41, "IK", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                43, "LOWERCASE", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));

        DdlException analyzerException = Assertions.assertThrows(
                DdlException.class, () -> manager.validateAnalyzerExists("IK"));
        Assertions.assertTrue(analyzerException.getMessage().contains("is not an analyzer"));
        Assertions.assertDoesNotThrow(() -> manager.validateAnalyzerExists("ik"));

        DdlException normalizerException = Assertions.assertThrows(
                DdlException.class, () -> manager.validateNormalizerExists("LOWERCASE"));
        Assertions.assertTrue(normalizerException.getMessage().contains("is not a normalizer"));
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("lowercase"));
    }

    @Test
    public void testNormalizerNamedAfterBuiltinAnalyzerIsUnreachable() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                45, "ik", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                46, "none", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                47, "norm_ascii", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));

        for (String name : List.of("ik", "IK", "none", " NONE ")) {
            DdlException error = Assertions.assertThrows(
                    DdlException.class, () -> manager.validateNormalizerExists(name));
            Assertions.assertTrue(error.getMessage().contains("built-in analyzer"), error.getMessage());
        }
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("norm_ascii"));
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("lowercase"));
    }

    @Test
    public void testExactCaseDistinctNormalizerPolicyRemainsReachable() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                48, "IK", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding")));

        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("IK"));
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("ik"));
    }

    @Test
    public void testCreateNormalizerPolicyRejectsBuiltinAnalyzerName() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        for (String name : List.of("ik", "IK", "none", "standard")) {
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> manager.createIndexPolicy(false, name, IndexPolicyTypeEnum.NORMALIZER,
                            new HashMap<>(Map.of("token_filter", "asciifolding"))));
            Assertions.assertTrue(error.getMessage().contains("conflicts with built-in"), error.getMessage());
        }
    }

    @Test
    public void testReplayedAnalyzerUsesExactTokenizerBinding() throws Exception {
        Map<String, String> invalidNgram = Map.of(
                "type", "ngram", "min_gram", "1", "max_gram", "3", "max_ngram_diff", "1");
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                50, "Foo", IndexPolicyTypeEnum.TOKENIZER, invalidNgram));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                51, "foo", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                52, "invalid_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "Foo")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                60, "Bar", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                61, "bar", IndexPolicyTypeEnum.TOKENIZER, invalidNgram));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                62, "valid_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "Bar")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                70, "Baz", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                71, "baz", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                72, "wrong_type_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "Baz")));

        IndexPolicyMgr restored = roundTrip(manager);
        DdlException invalidException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateAnalyzerExists("invalid_exact_tokenizer_analyzer"));
        Assertions.assertTrue(invalidException.getMessage().contains("invalid tokenizer 'Foo'"));
        Assertions.assertDoesNotThrow(
                () -> restored.validateAnalyzerExists("valid_exact_tokenizer_analyzer"));
        DdlException typeException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateAnalyzerExists("wrong_type_exact_tokenizer_analyzer"));
        Assertions.assertTrue(typeException.getMessage().contains("expected TOKENIZER"));
    }

    @Test
    public void testReplayedPoliciesRejectWrongExactNestedFilterTypes() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                80, "AnalyzerToken", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                81, "analyzertoken", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                82, "wrong_analyzer_token_filter", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "AnalyzerToken")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                90, "AnalyzerChar", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                91, "analyzerchar", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                92, "wrong_analyzer_char_filter", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "AnalyzerChar")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                100, "NormalizerToken", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                101, "normalizertoken", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                102, "wrong_normalizer_token_filter", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "NormalizerToken")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                110, "NormalizerChar", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                111, "normalizerchar", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                112, "wrong_normalizer_char_filter", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("char_filter", "NormalizerChar")));

        IndexPolicyMgr restored = roundTrip(manager);
        DdlException analyzerTokenException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateAnalyzerExists("wrong_analyzer_token_filter"));
        Assertions.assertTrue(analyzerTokenException.getMessage().contains("expected TOKEN_FILTER"));
        DdlException analyzerCharException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateAnalyzerExists("wrong_analyzer_char_filter"));
        Assertions.assertTrue(analyzerCharException.getMessage().contains("expected CHAR_FILTER"));
        DdlException normalizerTokenException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateNormalizerExists("wrong_normalizer_token_filter"));
        Assertions.assertTrue(normalizerTokenException.getMessage().contains("expected TOKEN_FILTER"));
        DdlException normalizerCharException = Assertions.assertThrows(DdlException.class,
                () -> restored.validateNormalizerExists("wrong_normalizer_char_filter"));
        Assertions.assertTrue(normalizerCharException.getMessage().contains("expected CHAR_FILTER"));
    }

    @Test
    public void testIfNotExistsKeepsReplayedBuiltinTokenizerNameIdempotent() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy replayed = new IndexPolicy(
                42, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));
        manager.replayCreateIndexPolicy(replayed);

        Assertions.assertDoesNotThrow(() -> manager.createIndexPolicy(
                true, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Assertions.assertSame(replayed, manager.getPolicyByName("ik_smart"));

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> new IndexPolicyMgr().createIndexPolicy(
                        true, "ik_max_word", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Assertions.assertTrue(exception.getMessage().contains("conflicts with built-in tokenizer name"));
    }

    @Test
    public void testNamedIkTokenizerPolicyValidation() throws Exception {
        Method validate = IndexPolicyMgr.class.getDeclaredMethod(
                "validateTokenizerProperties", Map.class);
        validate.setAccessible(true);
        IndexPolicyMgr manager = new IndexPolicyMgr();
        Assertions.assertDoesNotThrow(() -> validate.invoke(manager, Map.of("type", "ik_smart")));
        Assertions.assertDoesNotThrow(() -> validate.invoke(manager, Map.of("type", "ik_max_word")));
    }

    @Test
    public void testExistingPolicyPrecedesBuiltinAfterReplay() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                42, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Method validate = IndexPolicyMgr.class.getDeclaredMethod(
                "validatePolicyReference", String.class, IndexPolicyTypeEnum.class);
        validate.setAccessible(true);
        Assertions.assertDoesNotThrow(
                () -> validate.invoke(manager, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
    }

    @Test
    public void testBuiltinIkValidationIsLocaleIndependent() throws Exception {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            IndexPolicyMgr manager = new IndexPolicyMgr();
            Method validate = IndexPolicyMgr.class.getDeclaredMethod(
                    "validatePolicyReference", String.class, IndexPolicyTypeEnum.class);
            validate.setAccessible(true);
            Assertions.assertDoesNotThrow(
                    () -> validate.invoke(manager, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @Test
    public void testReplayDropPreservesSurvivingLocaleCollision() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy older = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword"));

        manager.replayCreateIndexPolicy(older);
        manager.replayCreateIndexPolicy(newer);
        Assertions.assertEquals(2, manager.getCopiedIndexPolicies().size());
        Assertions.assertTrue(manager.getCopiedIndexPolicies().containsAll(List.of(older, newer)));
        Assertions.assertEquals(older.getId(), manager.getPolicyByName("IK_SMART").getId());
        manager.replayDropIndexPolicy(new DropIndexPolicyLog(older.getId()));

        Assertions.assertEquals(newer.getId(), manager.getPolicyByName("IK_SMART").getId());
        Assertions.assertEquals(List.of(newer), manager.getCopiedIndexPolicies());
    }

    @Test
    public void testReplayDropRestoresOlderLocaleCollision() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy older = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword"));

        manager.replayCreateIndexPolicy(older);
        manager.replayCreateIndexPolicy(newer);
        manager.replayDropIndexPolicy(new DropIndexPolicyLog(newer.getId()));

        Assertions.assertEquals(older.getId(), manager.getPolicyByName("ik_smart").getId());
        Assertions.assertEquals(List.of(older), manager.getCopiedIndexPolicies());
    }

    @Test
    public void testImageRebuildPreservesLegacyExactNameBindings() throws Exception {
        long newerId = 1L << 32;
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy newer = new IndexPolicy(
                newerId, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword"));
        IndexPolicy older = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));

        manager.replayCreateIndexPolicy(newer);
        manager.replayCreateIndexPolicy(older);
        IndexPolicyMgr restored = roundTrip(manager);

        Assertions.assertEquals(older.getId(), restored.getPolicyByName("IK_SMART").getId());
        Assertions.assertEquals(newerId, restored.getPolicyByName("ik_smart").getId());
        Assertions.assertEquals(newerId, restored.getPolicyByName("Ik_Smart").getId());
    }

    @Test
    public void testJournalAndImageKeepLegacyExactNameBindings() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy historical = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword"));
        IndexPolicy dependent = new IndexPolicy(
                3, "legacy_exact_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "IK_SMART"));

        manager.replayCreateIndexPolicy(historical);
        manager.replayCreateIndexPolicy(newer);
        manager.replayCreateIndexPolicy(dependent);
        Assertions.assertEquals(historical.getId(), manager.getPolicyByName("IK_SMART").getId());
        Assertions.assertEquals(newer.getId(), manager.getPolicyByName("ik_smart").getId());
        Assertions.assertEquals(3, manager.getCopiedIndexPolicies().size());

        IndexPolicyMgr restored = roundTrip(manager);
        Assertions.assertEquals(historical.getId(), restored.getPolicyByName("IK_SMART").getId());
        Assertions.assertEquals(newer.getId(), restored.getPolicyByName("ik_smart").getId());
        Assertions.assertEquals("IK_SMART",
                restored.getPolicyByName(dependent.getName()).getProperties().get("tokenizer"));
        Assertions.assertEquals(3, restored.getCopiedIndexPolicies().size());
    }

    @Test
    public void testExactLegacyNameControlsValidationAndDropDependencies() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy exactAnalyzer = new IndexPolicy(
                10, "LEGACY_ANALYZER", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword"));
        IndexPolicy normalizedNormalizer = new IndexPolicy(
                11, "legacy_analyzer", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "lowercase"));
        IndexPolicy historicalTokenizer = new IndexPolicy(
                20, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard"));
        IndexPolicy normalizedTokenizer = new IndexPolicy(
                21, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword"));
        IndexPolicy dependentAnalyzer = new IndexPolicy(
                22, "legacy_exact_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "IK_SMART"));

        manager.replayCreateIndexPolicy(exactAnalyzer);
        manager.replayCreateIndexPolicy(normalizedNormalizer);
        manager.replayCreateIndexPolicy(historicalTokenizer);
        manager.replayCreateIndexPolicy(normalizedTokenizer);
        manager.replayCreateIndexPolicy(dependentAnalyzer);

        Assertions.assertDoesNotThrow(() -> manager.validateAnalyzerExists("LEGACY_ANALYZER"));
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("legacy_analyzer"));

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertDoesNotThrow(() -> manager.dropIndexPolicy(
                    false, "ik_smart", IndexPolicyTypeEnum.TOKENIZER));
            Assertions.assertEquals(historicalTokenizer.getId(), manager.getPolicyByName("ik_smart").getId());
            Assertions.assertThrows(DdlException.class, () -> manager.dropIndexPolicy(
                    false, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
        }
    }

    @Test
    public void testCanonicalBuiltinAnalyzerWinsValidationOverExactLegacyPolicy() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                30, "ik", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                31, "legacy_grams", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "common_grams")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                32, "standard", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "legacy_grams")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                33, "English", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                34, "lowercase", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));

        Assertions.assertAll(
                () -> Assertions.assertDoesNotThrow(() -> manager.validateAnalyzerExists("ik")),
                () -> Assertions.assertDoesNotThrow(() -> manager.validateAnalyzerExists("standard")),
                () -> Assertions.assertTrue(Assertions.assertThrows(DdlException.class,
                        () -> manager.validateAnalyzerExists("English")).getMessage()
                        .contains("is not an analyzer")),
                () -> Assertions.assertTrue(Assertions.assertThrows(DdlException.class,
                        () -> manager.validateNormalizerExists("lowercase")).getMessage()
                        .contains("is not a normalizer")));
    }

    @Test
    public void testDropDependencyFollowsTopLevelBuiltinPrecedence() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy upperIk = new IndexPolicy(
                40, "IK", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword"));
        IndexPolicy upperLowercase = new IndexPolicy(
                41, "LOWERCASE", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding"));
        IndexPolicy exactLowercase = new IndexPolicy(
                42, "lowercase", IndexPolicyTypeEnum.NORMALIZER, Map.of("token_filter", "asciifolding"));
        OlapTable table = new OlapTable();
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTables()).thenReturn(List.of(table));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(List.of(db));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            manager.replayCreateIndexPolicy(upperIk);
            manager.replayCreateIndexPolicy(upperLowercase);
            table.setIndexes(List.of(invertedIndex(1, "analyzer", "ik"), invertedIndex(2, "normalizer", "lowercase")));
            Assertions.assertDoesNotThrow(() -> manager.dropIndexPolicy(false, "IK", IndexPolicyTypeEnum.ANALYZER));
            Assertions.assertDoesNotThrow(
                    () -> manager.dropIndexPolicy(false, "LOWERCASE", IndexPolicyTypeEnum.NORMALIZER));

            manager.replayCreateIndexPolicy(upperIk);
            manager.replayCreateIndexPolicy(exactLowercase);
            table.setIndexes(List.of(invertedIndex(3, "analyzer", "IK"), invertedIndex(4, "normalizer", "lowercase")));
            Assertions.assertAll(
                    () -> Assertions.assertTrue(Assertions.assertThrows(DdlException.class,
                            () -> manager.dropIndexPolicy(false, "IK", IndexPolicyTypeEnum.ANALYZER))
                            .getMessage().contains("is used by index")),
                    () -> Assertions.assertTrue(Assertions.assertThrows(DdlException.class,
                            () -> manager.dropIndexPolicy(false, "lowercase", IndexPolicyTypeEnum.NORMALIZER))
                            .getMessage().contains("is used by index")));
        }
    }

    private static Index invertedIndex(long id, String key, String name) {
        return new Index(id, "idx_" + id, List.of("content"), IndexType.INVERTED, Map.of(key, name), "");
    }

    // StandardTokenizerValidator Tests
    @Test
    public void testStandardTokenizerValidator_ValidProperties() throws Exception {
        StandardTokenizerValidator validator = new StandardTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_token_length", "100");
        validator.validate(props); // Should not throw
    }

    @Test
    public void testStandardTokenizerValidator_InvalidMaxTokenLength() {
        StandardTokenizerValidator validator = new StandardTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_token_length", "0");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("must be a positive integer"));
    }

    // WordDelimiterTokenFilterValidator Tests
    // @Test
    // public void testWordDelimiterValidator_ValidProperties() throws Exception {
    //     WordDelimiterTokenFilterValidator validator = new WordDelimiterTokenFilterValidator();
    //     Map<String, String> props = new HashMap<>();
    //     props.put("catenate_words", "true");
    //     props.put("generate_word_parts", "false");
    //     props.put("type_table", "[a => ALPHA], [1 => DIGIT]");
    //     validator.validate(props); // Should not throw
    // }

    @Test
    public void testWordDelimiterValidator_InvalidBooleanValue() {
        WordDelimiterTokenFilterValidator validator = new WordDelimiterTokenFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("generate_word_parts", "yes");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("must be a boolean value"));
    }

    @Test
    public void testWordDelimiterValidator_InvalidTypeTableFormat() {
        WordDelimiterTokenFilterValidator validator = new WordDelimiterTokenFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("type_table", "a => ALPHA"); // Missing brackets

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("enclosed in square brackets"));
    }

    @Test
    public void testWordDelimiterValidator_InvalidTypeTableValue() {
        WordDelimiterTokenFilterValidator validator = new WordDelimiterTokenFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("type_table", "[a => INVALID]");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("Invalid type_table type"));
    }

    // Base Validator Tests
    @Test
    public void testBaseValidator_NullProperties() {
        AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(null));
        Assertions.assertTrue(exception.getMessage().contains("Properties cannot be null"));
    }

    @Test
    public void testBaseValidator_UnknownProperty() {
        AsciiFoldingTokenFilterValidator validator = new AsciiFoldingTokenFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("unknown_property", "value");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("does not support parameter"));
    }

    @Test
    public void testCharGroupTokenizer_ValidProperties() throws Exception {
        CharGroupTokenizerValidator validator = new CharGroupTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("max_token_length", "255");
        props.put("tokenize_on_chars", "[whitespace], [punctuation]");
        validator.validate(props); // Should not throw
    }

    @Test
    public void testCharGroupTokenizer_InvalidTokenizeOnChars_NoBrackets() {
        CharGroupTokenizerValidator validator = new CharGroupTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("tokenize_on_chars", "[whitespace], punctuation"); // second item missing brackets

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage().contains("enclosed in square brackets"));
    }

    @Test
    public void testCharReplaceCharFilterValidator_RejectsNonAsciiReplacement() {
        CharReplaceCharFilterValidator validator = new CharReplaceCharFilterValidator();
        Map<String, String> props = new HashMap<>();
        props.put("type", "char_replace");
        props.put("pattern", ".");
        props.put("replacement", "é");

        Exception exception = Assertions.assertThrows(DdlException.class,
                () -> validator.validate(props));
        Assertions.assertTrue(exception.getMessage()
                .contains("'char_filter_replacement' must contain only ASCII characters"));
    }
}
