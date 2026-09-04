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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
// import org.junit.jupiter.params.ParameterizedTest;
// import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
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
        validator.validate(props); // Should not throw
    }

    // NGramTokenizerValidator gram-mode (auto/sparse/dense) Tests
    @Test
    public void testNGramValidator_GramModeSparse() throws DdlException {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> props = new HashMap<>();
        props.put("type", "ngram");
        props.put("mode", "sparse");
        props.put("min_gram", "3");
        props.put("max_gram", "16");
        props.put("density", "0.25");
        props.put("stop_gram_df", "0.10");
        props.put("lower_case", "true");
        validator.validate(props);   // 不抛
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

        Map<String, String> wideGap = new HashMap<>();   // mode 存在时允许 max-min>1
        wideGap.put("type", "ngram");
        wideGap.put("mode", "sparse");
        wideGap.put("min_gram", "3");
        wideGap.put("max_gram", "24");
        Assertions.assertDoesNotThrow(() -> validator.validate(wideGap));
    }

    @Test
    public void testNGramValidator_GramModeRejectsEmptyMode() {
        // BE 将空 mode 当作 legacy 处理，但 FE 校验必须在 DDL 阶段就拒绝空字符串的 mode 取值。
        NGramTokenizerValidator validator = new NGramTokenizerValidator();
        Map<String, String> emptyMode = new HashMap<>();
        emptyMode.put("type", "ngram");
        emptyMode.put("mode", "");
        DdlException e = Assertions.assertThrows(DdlException.class, () -> validator.validate(emptyMode));
        Assertions.assertTrue(e.getMessage().contains("mode must be one of"));
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
