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
        Assertions.assertTrue(e.getMessage().contains("mode must be one of"), e.getMessage());
        // 空取值必须在报错信息里可辨认，不能只留下一个空白的 "got: "
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

    /**
     * gram 族参数的取值域必须与 BE `gram_scheme.cpp::from_properties` 一致：
     * min_gram ∈ [1, 64]、max_gram ∈ [1, 256]、density ∈ [0.001, 1]、stop_gram_df ∈ [0, 1]。
     * FE 放过越界值只会把错误推迟到写入时的 BE InvalidArgument。
     */
    @Test
    public void testNGramValidator_GramModeValueDomainsMirrorBe() {
        NGramTokenizerValidator validator = new NGramTokenizerValidator();

        Map<String, String> maxGramTooBig = sparseGramProps();
        maxGramTooBig.put("max_gram", "257");                     // BE 上界是 256
        String maxGramMessage = assertGramPropRejected(maxGramTooBig);
        Assertions.assertTrue(maxGramMessage.contains("max_gram must be an integer in [1, 256]"), maxGramMessage);

        Map<String, String> minGramTooBig = sparseGramProps();
        minGramTooBig.put("min_gram", "65");                      // BE 上界是 64
        String minGramMessage = assertGramPropRejected(minGramTooBig);
        Assertions.assertTrue(minGramMessage.contains("min_gram must be an integer in [1, 64]"), minGramMessage);

        Map<String, String> gramAtBound = sparseGramProps();      // 边界值本身仍必须被接受
        gramAtBound.put("min_gram", "64");
        gramAtBound.put("max_gram", "256");
        Assertions.assertDoesNotThrow(() -> validator.validate(gramAtBound));

        Map<String, String> densityTooSmall = sparseGramProps();
        densityTooSmall.put("density", "0.0005");                 // BE 下界是 0.001（千分比取整）
        String densityMessage = assertGramPropRejected(densityTooSmall);
        Assertions.assertTrue(densityMessage.contains("density must be in [0.001, 1]"), densityMessage);

        Map<String, String> densityAtBound = sparseGramProps();
        densityAtBound.put("density", "0.001");
        Assertions.assertDoesNotThrow(() -> validator.validate(densityAtBound));

        Map<String, String> stopGramDfTooBig = sparseGramProps();
        stopGramDfTooBig.put("stop_gram_df", "1.5");
        String stopGramDfMessage = assertGramPropRejected(stopGramDfTooBig);
        Assertions.assertTrue(stopGramDfMessage.contains("stop_gram_df must be in [0, 1]"), stopGramDfMessage);

        Map<String, String> badLowerCase = sparseGramProps();
        badLowerCase.put("lower_case", "yes");
        String lowerCaseMessage = assertGramPropRejected(badLowerCase);
        Assertions.assertTrue(lowerCaseMessage.contains("lower_case must be true or false"), lowerCaseMessage);

        Map<String, String> inverted = sparseGramProps();         // 带 mode 时同样要守 min <= max
        inverted.put("min_gram", "5");
        inverted.put("max_gram", "4");
        String invertedMessage = assertGramPropRejected(inverted);
        Assertions.assertTrue(invertedMessage.contains("min_gram (5) must be <= max_gram (4)"), invertedMessage);
    }

    /**
     * mode 取值不做 trim、也不折叠大小写：BE `from_properties` 做的是精确字符串比较，
     * FE 若接受 " Sparse " 就会原样落盘、写入时才在 BE 报 InvalidArgument。
     * 这里锁定「FE 直接拒绝」这一裁决（另一种可选实现是 FE 归一化后再落盘，本实现不采用）。
     */
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
