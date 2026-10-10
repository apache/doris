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

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
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
        validator.validate(ImmutableMap.of("type", "asciifolding", "preserve_original", "true"));
        validator.validate(ImmutableMap.of("type", "asciifolding", "preserve_original", "false"));
    }

    private static IndexPolicy roundTrip(IndexPolicy policy) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        policy.write(new DataOutputStream(bytes));
        return IndexPolicy.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
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
    public void testNGramValidator_GramModeSparse() throws DdlException {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("type", "ngram");
        props.put("mode", "sparse");
        props.put("min_gram", "3");
        props.put("max_gram", "16");
        props.put("density", "0.25");
        props.put("lower_case", "true");
        validator.validate(props);
    }

    @Test
    public void testNGramValidator_GramModeRejectsMaxNgramDiff() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = sparseGramProps();
        props.put("max_ngram_diff", "7");
        DdlException e = Assertions.assertThrows(DdlException.class, () -> validator.validate(props));
        Assertions.assertTrue(e.getMessage().contains("max_ngram_diff cannot be used together with mode"));
    }

    @Test
    public void testNGramValidator_GramModeRejectsBadValues() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> bad = new HashMap<>();
        bad.put("type", "ngram");
        bad.put("mode", "fuzzy");
        DdlException e1 = Assertions.assertThrows(DdlException.class, () -> validator.validate(bad));
        Assertions.assertTrue(e1.getMessage().contains("mode must be one of"));

        Map<String, String> noMode = new HashMap<>();
        noMode.put("type", "ngram");
        noMode.put("density", "0.25");
        DdlException e2 = Assertions.assertThrows(DdlException.class, () -> validator.validate(noMode));
        Assertions.assertTrue(e2.getMessage().contains("requires mode"));

        Map<String, String> badDensity = new HashMap<>();
        badDensity.put("type", "ngram");
        badDensity.put("mode", "sparse");
        badDensity.put("density", "1.5");
        Assertions.assertTrue(Assertions.assertThrows(DdlException.class, () -> validator.validate(badDensity))
                .getMessage().contains("density must be"));

        Map<String, String> tokenChars = new HashMap<>();
        tokenChars.put("type", "ngram");
        tokenChars.put("mode", "dense");
        tokenChars.put("token_chars", "letter");
        Assertions.assertTrue(Assertions.assertThrows(DdlException.class, () -> validator.validate(tokenChars))
                .getMessage().contains("token_chars cannot be used"));

        Map<String, String> wideGap = new HashMap<>();
        wideGap.put("type", "ngram");
        wideGap.put("mode", "sparse");
        wideGap.put("min_gram", "3");
        wideGap.put("max_gram", "24");
        Assertions.assertDoesNotThrow(() -> validator.validate(wideGap));
    }

    @Test
    public void testNGramValidator_GramModeRejectsEmptyMode() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> emptyMode = new HashMap<>();
        emptyMode.put("type", "ngram");
        emptyMode.put("mode", "");
        DdlException e = Assertions.assertThrows(DdlException.class, () -> validator.validate(emptyMode));
        Assertions.assertTrue(e.getMessage().contains("mode must be one of"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("got: '' (empty)"), e.getMessage());
    }

    private static Map<String, String> sparseGramProps() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "ngram");
        props.put("mode", "sparse");
        return props;
    }

    private static String assertGramPropRejected(Map<String, String> props) {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        return Assertions.assertThrows(DdlException.class, () -> validator.validate(props)).getMessage();
    }

    @Test
    public void testNGramValidator_GramModeValueDomainsMirrorBe() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();

        Map<String, String> maxGramTooBig = sparseGramProps();
        maxGramTooBig.put("max_gram", "257");
        String maxGramMessage = assertGramPropRejected(maxGramTooBig);
        Assertions.assertTrue(maxGramMessage.contains("max_gram must be an integer in [1, 256]"), maxGramMessage);

        Map<String, String> minGramTooBig = sparseGramProps();
        minGramTooBig.put("min_gram", "65");
        String minGramMessage = assertGramPropRejected(minGramTooBig);
        Assertions.assertTrue(minGramMessage.contains("min_gram must be an integer in [1, 64]"), minGramMessage);

        Map<String, String> gramAtBound = sparseGramProps();
        gramAtBound.put("min_gram", "64");
        gramAtBound.put("max_gram", "256");
        Assertions.assertDoesNotThrow(() -> validator.validate(gramAtBound));

        Map<String, String> densityTooSmall = sparseGramProps();
        densityTooSmall.put("density", "0.0005");
        String densityMessage = assertGramPropRejected(densityTooSmall);
        Assertions.assertTrue(densityMessage.contains("density must be in [0.001, 1]"), densityMessage);

        Map<String, String> densityAtBound = sparseGramProps();
        densityAtBound.put("density", "0.001");
        Assertions.assertDoesNotThrow(() -> validator.validate(densityAtBound));

        Map<String, String> stopGramDf = sparseGramProps();
        stopGramDf.put("stop_gram_df", "0.10");
        String stopGramDfMessage = assertGramPropRejected(stopGramDf);
        Assertions.assertTrue(stopGramDfMessage.contains("stop_gram_df"), stopGramDfMessage);

        Map<String, String> badLowerCase = sparseGramProps();
        badLowerCase.put("lower_case", "yes");
        String lowerCaseMessage = assertGramPropRejected(badLowerCase);
        Assertions.assertTrue(lowerCaseMessage.contains("lower_case must be true or false"), lowerCaseMessage);

        Map<String, String> inverted = sparseGramProps();
        inverted.put("min_gram", "5");
        inverted.put("max_gram", "4");
        String invertedMessage = assertGramPropRejected(inverted);
        Assertions.assertTrue(invertedMessage.contains("min_gram (5) must be <= max_gram (4)"), invertedMessage);
    }

    @Test
    public void testNGramValidator_GramIntegerPropsRejectNonAsciiDigits() {
        Map<String, String> fullWidthMin = sparseGramProps();
        fullWidthMin.put("min_gram", "３");
        String minMessage = assertGramPropRejected(fullWidthMin);
        Assertions.assertTrue(minMessage.contains("min_gram must be an integer in [1, 64]"), minMessage);

        Map<String, String> fullWidthMax = sparseGramProps();
        fullWidthMax.put("max_gram", "１６");
        String maxMessage = assertGramPropRejected(fullWidthMax);
        Assertions.assertTrue(maxMessage.contains("max_gram must be an integer in [1, 256]"), maxMessage);

        Map<String, String> arabicIndic = sparseGramProps();
        arabicIndic.put("min_gram", "٣");
        String arabicMessage = assertGramPropRejected(arabicIndic);
        Assertions.assertTrue(arabicMessage.contains("min_gram must be an integer in [1, 64]"), arabicMessage);

        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> ascii = sparseGramProps();
        ascii.put("min_gram", "3");
        ascii.put("max_gram", "+16");
        Assertions.assertDoesNotThrow(() -> validator.validate(ascii));
    }

    @Test
    public void testNGramValidator_GramModeRejectsUntrimmedAndMixedCase() {
        Map<String, String> padded = new HashMap<>();
        padded.put("type", "ngram");
        padded.put("mode", " Sparse ");
        String message = assertGramPropRejected(padded);
        Assertions.assertTrue(message.contains("mode must be one of"), message);
        Assertions.assertTrue(message.contains("got: ' Sparse '"), message);

        Map<String, String> upper = new HashMap<>();
        upper.put("type", "ngram");
        upper.put("mode", "SPARSE");
        String upperMessage = assertGramPropRejected(upper);
        Assertions.assertTrue(upperMessage.contains("mode must be one of"), upperMessage);
    }

    @Test
    public void testNGramValidator_GramDecimalPropertiesHavePortableSyntax() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        for (String key : new String[] {"density"}) {
            for (String value : new String[] {"0.25f", "0.25D", "0.25 ", " 0.25", "0.25\t",
                    "0x1p-2", "NaN", "Infinity", "", ".", "1e", "０.２５"}) {
                Map<String, String> props = sparseGramProps();
                props.put(key, value);
                Assertions.assertThrows(DdlException.class, () -> validator.validate(props),
                        key + "=" + value);
            }
            for (String value : new String[] {"0.25", ".25", "1.", "+0.25", "2.5e-1", "0.001", "1"}) {
                Map<String, String> props = sparseGramProps();
                props.put(key, value);
                Assertions.assertDoesNotThrow(() -> validator.validate(props), key + "=" + value);
            }
        }
    }

    @Test
    public void testGramPolicyRemainsValidAfterReplay() throws Exception {
        Map<String, String> props = sparseGramProps();
        props.put("min_gram", "3");
        props.put("max_gram", "16");
        IndexPolicy replayed = roundTrip(new IndexPolicy(
                3, "gram_without_marker", IndexPolicyTypeEnum.TOKENIZER, props));
        Assertions.assertFalse(replayed.isInvalid());
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

    private static IndexPolicyMgr roundTrip(IndexPolicyMgr manager) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        manager.write(new DataOutputStream(bytes));
        return IndexPolicyMgr.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
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
                41, "IK", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                43, "LOWERCASE", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));

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
                45, "ik", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "asciifolding")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                46, "none", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                47, "norm_ascii", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "asciifolding")));

        for (String name : ImmutableList.of("ik", "IK", "none", " NONE ")) {
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
                48, "IK", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "asciifolding")));

        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("IK"));
        Assertions.assertDoesNotThrow(() -> manager.validateNormalizerExists("ik"));
    }

    @Test
    public void testCreateNormalizerPolicyRejectsBuiltinAnalyzerName() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        for (String name : ImmutableList.of("ik", "IK", "none", "standard")) {
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> manager.createIndexPolicy(false, name, IndexPolicyTypeEnum.NORMALIZER,
                            new HashMap<>(ImmutableMap.of("token_filter", "asciifolding"))));
            Assertions.assertTrue(error.getMessage().contains("conflicts with built-in"), error.getMessage());
        }
    }

    @Test
    public void testReplayedAnalyzerUsesExactTokenizerBinding() throws Exception {
        Map<String, String> invalidNgram = ImmutableMap.of(
                "type", "ngram", "min_gram", "3", "max_gram", "1");
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                50, "Foo", IndexPolicyTypeEnum.TOKENIZER, invalidNgram));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                51, "foo", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                52, "invalid_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "Foo")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                60, "Bar", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                61, "bar", IndexPolicyTypeEnum.TOKENIZER, invalidNgram));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                62, "valid_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "Bar")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                70, "Baz", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                71, "baz", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                72, "wrong_type_exact_tokenizer_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "Baz")));

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
    public void testReplayedMalformedNgramTokenizerIsRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ngram");
        properties.put("token_chars", null);
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                90, "malformed_ngram", IndexPolicyTypeEnum.TOKENIZER, properties));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                91, "malformed_ngram_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "malformed_ngram")));

        Assertions.assertThrows(DdlException.class,
                () -> manager.validateAnalyzerExists("malformed_ngram_analyzer"));
    }

    @Test
    public void testReplayedPoliciesRejectWrongExactNestedFilterTypes() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                80, "AnalyzerToken", IndexPolicyTypeEnum.CHAR_FILTER, ImmutableMap.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                81, "analyzertoken", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                82, "wrong_analyzer_token_filter", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "keyword", "token_filter", "AnalyzerToken")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                90, "AnalyzerChar", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                91, "analyzerchar", IndexPolicyTypeEnum.CHAR_FILTER, ImmutableMap.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                92, "wrong_analyzer_char_filter", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "keyword", "char_filter", "AnalyzerChar")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                100, "NormalizerToken", IndexPolicyTypeEnum.CHAR_FILTER, ImmutableMap.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                101, "normalizertoken", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                102, "wrong_normalizer_token_filter", IndexPolicyTypeEnum.NORMALIZER,
                ImmutableMap.of("token_filter", "NormalizerToken")));

        manager.replayCreateIndexPolicy(new IndexPolicy(
                110, "NormalizerChar", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "lowercase")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                111, "normalizerchar", IndexPolicyTypeEnum.CHAR_FILTER, ImmutableMap.of("type", "char_replace")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                112, "wrong_normalizer_char_filter", IndexPolicyTypeEnum.NORMALIZER,
                ImmutableMap.of("char_filter", "NormalizerChar")));

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
                42, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));
        manager.replayCreateIndexPolicy(replayed);

        Assertions.assertDoesNotThrow(() -> manager.createIndexPolicy(
                true, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        Assertions.assertSame(replayed, manager.getPolicyByName("ik_smart"));

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> new IndexPolicyMgr().createIndexPolicy(
                        true, "ik_max_word", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
        Assertions.assertTrue(exception.getMessage().contains("conflicts with built-in tokenizer name"));
    }

    @Test
    public void testNamedIkTokenizerPolicyValidation() throws Exception {
        Method validate = IndexPolicyMgr.class.getDeclaredMethod(
                "validateTokenizerProperties", Map.class);
        validate.setAccessible(true);
        IndexPolicyMgr manager = new IndexPolicyMgr();
        Assertions.assertDoesNotThrow(() -> validate.invoke(manager, ImmutableMap.of("type", "ik_smart")));
        Assertions.assertDoesNotThrow(() -> validate.invoke(manager, ImmutableMap.of("type", "ik_max_word")));
    }

    @Test
    public void testExistingPolicyPrecedesBuiltinAfterReplay() throws Exception {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        manager.replayCreateIndexPolicy(new IndexPolicy(
                42, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard")));
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
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword"));

        manager.replayCreateIndexPolicy(older);
        manager.replayCreateIndexPolicy(newer);
        Assertions.assertEquals(2, manager.getCopiedIndexPolicies().size());
        Assertions.assertTrue(manager.getCopiedIndexPolicies().containsAll(ImmutableList.of(older, newer)));
        Assertions.assertEquals(older.getId(), manager.getPolicyByName("IK_SMART").getId());
        manager.replayDropIndexPolicy(new DropIndexPolicyLog(older.getId()));

        Assertions.assertEquals(newer.getId(), manager.getPolicyByName("IK_SMART").getId());
        Assertions.assertEquals(ImmutableList.of(newer), manager.getCopiedIndexPolicies());
    }

    @Test
    public void testReplayDropRestoresOlderLocaleCollision() {
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy older = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword"));

        manager.replayCreateIndexPolicy(older);
        manager.replayCreateIndexPolicy(newer);
        manager.replayDropIndexPolicy(new DropIndexPolicyLog(newer.getId()));

        Assertions.assertEquals(older.getId(), manager.getPolicyByName("ik_smart").getId());
        Assertions.assertEquals(ImmutableList.of(older), manager.getCopiedIndexPolicies());
    }

    @Test
    public void testImageRebuildPreservesLegacyExactNameBindings() throws Exception {
        long newerId = 1L << 32;
        IndexPolicyMgr manager = new IndexPolicyMgr();
        IndexPolicy newer = new IndexPolicy(
                newerId, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword"));
        IndexPolicy older = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));

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
                1, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));
        IndexPolicy newer = new IndexPolicy(
                2, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword"));
        IndexPolicy dependent = new IndexPolicy(
                3, "legacy_exact_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "IK_SMART"));

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
                10, "LEGACY_ANALYZER", IndexPolicyTypeEnum.ANALYZER, ImmutableMap.of("tokenizer", "keyword"));
        IndexPolicy normalizedNormalizer = new IndexPolicy(
                11, "legacy_analyzer", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "lowercase"));
        IndexPolicy historicalTokenizer = new IndexPolicy(
                20, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "standard"));
        IndexPolicy normalizedTokenizer = new IndexPolicy(
                21, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword"));
        IndexPolicy dependentAnalyzer = new IndexPolicy(
                22, "legacy_exact_analyzer", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "IK_SMART"));

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
                30, "ik", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                31, "legacy_grams", IndexPolicyTypeEnum.TOKEN_FILTER, ImmutableMap.of("type", "common_grams")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                32, "standard", IndexPolicyTypeEnum.ANALYZER,
                ImmutableMap.of("tokenizer", "keyword", "token_filter", "legacy_grams")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                33, "English", IndexPolicyTypeEnum.TOKENIZER, ImmutableMap.of("type", "keyword")));
        manager.replayCreateIndexPolicy(new IndexPolicy(
                34, "lowercase", IndexPolicyTypeEnum.ANALYZER, ImmutableMap.of("tokenizer", "keyword")));

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
                40, "IK", IndexPolicyTypeEnum.ANALYZER, ImmutableMap.of("tokenizer", "keyword"));
        IndexPolicy upperLowercase = new IndexPolicy(
                41, "LOWERCASE", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "asciifolding"));
        IndexPolicy exactLowercase = new IndexPolicy(
                42, "lowercase", IndexPolicyTypeEnum.NORMALIZER, ImmutableMap.of("token_filter", "asciifolding"));
        OlapTable table = new OlapTable();
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTables()).thenReturn(ImmutableList.of(table));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(ImmutableList.of(db));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            manager.replayCreateIndexPolicy(upperIk);
            manager.replayCreateIndexPolicy(upperLowercase);
            table.setIndexes(ImmutableList.of(invertedIndex(1, "analyzer", "ik"), invertedIndex(2, "normalizer", "lowercase")));
            Assertions.assertDoesNotThrow(() -> manager.dropIndexPolicy(false, "IK", IndexPolicyTypeEnum.ANALYZER));
            Assertions.assertDoesNotThrow(
                    () -> manager.dropIndexPolicy(false, "LOWERCASE", IndexPolicyTypeEnum.NORMALIZER));

            manager.replayCreateIndexPolicy(upperIk);
            manager.replayCreateIndexPolicy(exactLowercase);
            table.setIndexes(ImmutableList.of(invertedIndex(3, "analyzer", "IK"), invertedIndex(4, "normalizer", "lowercase")));
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
        return new Index(id, "idx_" + id, ImmutableList.of("content"), IndexType.INVERTED, ImmutableMap.of(key, name), "");
    }

}
