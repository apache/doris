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

package org.apache.doris.common;

import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.Lists;
import org.apache.ivy.util.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class FeNameFormatTest {

    @Test
    void testLabelName() {
        List<String> alwaysValid = Lists.newArrayList(
                "abc123",        // alphanumeric
                "A-B_C:D",       // contains all allowed special chars
                "0-1_2:3",       // starts with number, contains special chars
                "a",             // single character
                "X-Y-Z",         // hyphens and uppercase
                "test_123:456",  // mixed with underscore and colon
                "-valid",        // starts with hyphen
                "_valid",        // starts with underscore
                ":valid",        // starts with colon
                StringUtils.repeat("a", Config.label_regex_length)  // maximum length
        );

        List<String> alwaysInvalid = Lists.newArrayList(
                "",              // empty string
                " ",             // space character
                "a b",           // contains space
                "a.b",           // contains dot
                "a@b",           // contains @
                "a\nb",          // contains newline
                "a$b",           // contains $
                "a*b",           // contains *
                "a#b",           // contains #
                StringUtils.repeat("a", Config.label_regex_length + 1) // maximum length
        );

        List<String> unicodeValid = Lists.newArrayList(
                "äöü",          // German umlauts
                "北京",         // Chinese characters
                "東京123",      // Japanese with numbers
                "München",      // German city name
                "Beyoncé",      // French name
                "αβγ",          // Greek letters
                "русский",      // Russian letters
                "naïve",        // French word
                "Ḥello",        // special diacritic
                "øre",          // Nordic word
                "café",         // French word
                "ẞig"           // German sharp S
        );

        test(FeNameFormat::checkLabel, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void testTableName() {
        List<String> alwaysValid = Lists.newArrayList(
                "abc123",    // Starts with ASCII letter, contains alphanumerics + underscores
                "A_1_b",     // Contains allowed symbols (underscore)
                "Z",         // Single ASCII letter
                "a1b2c3",    // Alphanumeric combination
                "x_y_z",     // Contains underscores
                "test",      // Letters only
                "a_b_c",     // Multiple underscores
                "a_1",       // Underscore + number
                "B2",        // Uppercase letter + number
                "1abc",      // Starts with digit
                "abc$",      // Contains invalid symbol $
                "-abc",      // Starts with hyphen
                "_abc"       // Starts with underscore
        );

        List<String> alwaysInvalid = Lists.newArrayList(
                "",          // Empty string
                "x ",          // space character as last one
                "x\t",         // table character as last one
                "x\n"          // enter character as last one
        );

        List<String> unicodeValid = Lists.newArrayList(
                "@test",     // Contains invalid symbol @
                "a b",       // Contains space
                "a\tb",      // Contains space
                "a\nb",      // Contains space
                "a\rb",      // Contains space
                " ab",       // Contains space
                "a*b",       // Contains asterisk
                "a.b",       // Contains dot
                "a#b",       // Contains hash
                "abc!",      // Contains invalid symbol !
                "a\nb",      // Contains newline
                "éclair",    // Contains French letter
                "über",      // Contains German umlaut
                "北京",      // Chinese characters
                "東京123",   // Japanese characters + numbers
                "München",   // Contains umlaut
                "Beyoncé",   // Contains French accent
                "αβγ",       // Greek letters
                "русский",   // Cyrillic letters
                "øre",       // Nordic letter
                "ção",       // Portuguese letter
                "naïve",     // Contains diacritic
                "Ḥello",     // Contains special diacritic
                "ẞig"        // German sharp S
        );

        test(FeNameFormat::checkTableName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void testCheckColumnName() {
        List<String> alwaysValid = Lists.newArrayList(
                "_id",
                "_id",
                "_ id",
                " _id",
                "__id",
                "___id",
                "___id_",
                "mv_",
                "mva_",
                "@timestamp",
                "@timestamp#",
                "timestamp*",
                "timestamp.1",
                "timestamp.#",
                "?id_",
                "#id_",
                "$id_",
                "a-zA-Z0-9.+-/?@#$%^&*\" ,:",
                " x",
                "y x",
                "y\tx",
                "y\nx",
                "y\rx"
        );

        List<String> alwaysInvalid = Lists.newArrayList(
                // inner column prefix
                "__doris_shadow_",
                "",
                " ",
                "x ",
                "x\t",
                "x\n",
                "x\r",
                StringUtils.repeat("a", 257)
        );

        List<String> unicodeValid = Lists.newArrayList(
                "\\",
                "column\\",
                "中文",
                "語言",
                "язык",
                "언어",
                "لغة",
                "ภาษา",
                "שפה",
                "γλώσσα",
                "ენა",
                "げんご"
        );

        test(FeNameFormat::checkColumnName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void testUserName() {
        List<String> alwaysValid = Arrays.asList(
                "a",
                "abc123",
                "A-1_b.c",
                "x.y-z_123",
                "test",
                "a.b-c_d",
                "Z",
                "a-",
                "a_",
                "a."
        );
        List<String> alwaysInvalid = Arrays.asList(
                "1abc",      // starts with digit
                "@test",     // contains invalid character @
                "a b",       // contains space
                "",          // empty string
                "-abc",      // starts with hyphen
                ".abc",      // starts with dot
                "_abc",      // starts with underscore
                "abc!",      // contains invalid character !
                "abc\n",     // contains newline
                "9",         // digit only
                " ",         // whitespace only
                "a\tb",      // contains tab
                "a\nb",      // contains newline
                "a*",        // contains asterisk
                "a(",         // contains parenthesis
                "a:b",        // contains colon
                " ab"         // contains space
        );
        List<String> unicodeValid = Lists.newArrayList(
                "éclair",       // starts with accented letter
                "über",         // starts with umlaut
                "北京abc",       // starts with Chinese characters
                "東京123",       // starts with Japanese kanji
                "русский",      // starts with Cyrillic letters
                "αβγ.123",      // starts with Greek letters
                "München",      // contains umlaut
                "Beyoncé",      // contains accented letter
                "naïve"       // contains diacritic
        );
        test(FeNameFormat::checkUserName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void testCommonName() {
        List<String> alwaysValid = Arrays.asList(
                "abc123",    // ASCII letters + numbers
                "A-1_b",     // with allowed symbols (-_)
                "Z",         // single ASCII letter
                "a1b2c3",    // alphanumeric
                "x_y-z",     // underscore and hyphen
                "test",      // letters only
                "a-b-c",     // multiple hyphens
                "a_b",       // underscore
                "a-1",       // hyphen + number
                "B2"         // uppercase + number
        );
        List<String> alwaysInvalid = Arrays.asList(
                "1abc",      // starts with number
                "@test",     // contains invalid symbol @
                "",          // empty string
                "a b",       // contains space
                "abc!",      // contains invalid symbol !
                "a\nb",      // contains newline
                "abc$",      // contains invalid symbol $
                "-abc",      // starts with hyphen
                "_abc",      // starts with underscore
                StringUtils.repeat("a", 65), // exceeds length limit (64)
                "a*b",       // contains asterisk
                "a.b",       // contains dot (if not allowed)
                "a#b"        // contains hash symbol
        );
        List<String> unicodeValid = Lists.newArrayList(
                "éclair",    // French letters
                "über",      // German umlaut
                "北京",      // Chinese characters
                "東京123",   // Japanese + numbers
                "München",   // German umlaut
                "Beyoncé",   // French accent
                "αβγ",       // Greek letters
                "русский",   // Cyrillic letters
                "øre",       // Nordic letter
                "ção",       // Portuguese letter
                "naïve",     // French diacritic
                "Ḥello",     // special diacritic
                "ẞig"        // German sharp S
        );
        test(FeNameFormatTest::checkCommonName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void testOutfileName() {
        List<String> alwaysValid = Arrays.asList(
                "_valid",      // starts with underscore
                "a1_b-c",      // letters, numbers, hyphens, underscores
                "ValidName",   // standard camel case
                "x_123",       // underscore + numbers
                "A_B_C",       // multiple underscores
                "z",           // single letter
                "_9value",     // starts with _, contains number
                "a-b",         // simple hyphenated
                "MAX_LENGTH"   // uppercase with underscores
        );
        List<String> alwaysInvalid = Arrays.asList(
                "1invalid",    // starts with number
                "@test",       // invalid starting character
                "",            // empty string
                "has space",   // contains space
                "a.b",         // contains dot
                "a*b",         // contains asterisk
                "a\nb",        // contains newline
                "-invalid",     // starts with hyphen
                "a#b",         // contains hash
                StringUtils.repeat("a", 65), // exceeds length limit (64)
                "a>b",         // contains angle bracket
                "a$b"          // contains dollar sign
        );
        List<String> unicodeValid = Lists.newArrayList(
                "_éclair",     // starts with _, contains French letter
                "über",        // German umlaut
                "_北京",       // starts with _, Chinese characters
                "東京123",     // Japanese + numbers
                "München",     // German umlaut
                "Beyoncé",     // French accent
                "αβγ",         // Greek letters
                "_русский",    // starts with _, Cyrillic letters
                "naïve",       // French diacritic
                "Ḥello",       // special diacritic
                "ẞig",         // German sharp S
                "øre",         // Nordic letter
                "_ção"         // starts with _, Portuguese letter
        );
        test(FeNameFormatTest::checkOutfileSuccessFileName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    @Test
    void checkRepositoryName() {
        List<String> alwaysValid = Arrays.asList(
                "validName",      // Standard ASCII letters
                "A1_b2",         // Letters, numbers, underscore
                "Product123",     // CamelCase with numbers
                "x_y_z",         // Multiple underscores
                "test",          // Lowercase only
                "MAX_LENGTH",    // Uppercase with underscores
                "a1-b2",         // Hyphen included
                "Z",             // Single character
                StringUtils.repeat("a", 256)  // Maximum length (255 chars)
        );
        List<String> alwaysInvalid = Arrays.asList(
                "",               // Empty string
                " ",              // Space character
                "1stInvalid",     // Starts with number
                "@username",      // Invalid starting character
                "name with space", // Contains space
                "a.b",            // Contains dot
                "a*b",            // Contains asterisk
                "a\nb",           // Contains newline
                "a$b",            // Contains dollar sign
                "a#b",            // Contains hash
                StringUtils.repeat("a", 257),  // Exceeds length limit (255)
                "-invalid"        // Starts with hyphen
        );
        List<String> unicodeValid = Lists.newArrayList(
                "München",       // German umlaut
                "Beyoncé",       // French accent
                "東京太郎",      // Japanese characters
                "北京123",       // Chinese with numbers
                "αβγ",           // Greek letters
                "русский",       // Cyrillic letters
                "naïve",         // French diacritic
                "øre",           // Nordic letter
                "café",          // French word
                "Ḥello",         // Special diacritic
                "ẞig",           // German sharp S
                "SãoPaulo"       // Portuguese
        );
        test(FeNameFormat::checkRepositoryName, alwaysValid, alwaysInvalid, unicodeValid);
    }

    private static void checkOutfileSuccessFileName(String name) throws AnalysisException {
        FeNameFormat.checkOutfileSuccessFileName("fakeType", name);
    }

    private static void checkCommonName(String name) throws AnalysisException {
        FeNameFormat.checkCommonName("fakeType", name);
    }

    @FunctionalInterface
    public interface NameValidator {
        void accept(String name) throws AnalysisException;
    }

    private void test(NameValidator validator, List<String> alwaysValid, List<String> alwaysInvalid,
            List<String> unicodeValid) {
        boolean defaultUnicode = VariableMgr.getDefaultSessionVariable().enableUnicodeNameSupport;
        List<Boolean> enableUnicode = Lists.newArrayList(false, true);
        try {
            for (Boolean unicode : enableUnicode) {
                VariableMgr.getDefaultSessionVariable().setEnableUnicodeNameSupport(unicode);
                for (String s : alwaysValid) {
                    ExceptionChecker.expectThrowsNoException(() -> validator.accept(s));
                }
                for (String s : alwaysInvalid) {
                    Assertions.assertThrowsExactly(AnalysisException.class, () -> validator.accept(s),
                            "name should be invalid: " + s);
                }
                for (String s : unicodeValid) {
                    if (unicode) {
                        ExceptionChecker.expectThrowsNoException(() -> validator.accept(s));
                    } else {
                        Assertions.assertThrowsExactly(AnalysisException.class, () -> validator.accept(s),
                                "name should be invalid: " + s);

                    }
                }
            }
        } finally {
            VariableMgr.getDefaultSessionVariable().setEnableUnicodeNameSupport(defaultUnicode);
        }
    }
}
