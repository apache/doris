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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class FeNameFormatTest {

    @Test
    void testLabelName() {
        // check label use correct regex, begin with '-' is different from others
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkLabel("-lable"));
    }

    @Test
    void testTableName() {
        // length 64
        String tblName = "test_sys_partition_list_basic_test_list_partition_bigint_tb-uniq";
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkTableName(tblName));
        // length 70
        String largeTblName = "test_sys_partition_list_basic_test_list_partition_bigint_tb_uniq_large";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkTableName(largeTblName));
        // check table name use correct regex, not begin with '-'
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkTableName("-" + tblName));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkTableName("a:b"));
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
                "@timestamp",
                "@timestamp#",
                "timestamp*",
                "timestamp.1",
                "timestamp.#",
                "?id_",
                "#id_",
                "$id_",
                "a-zA-Z0-9.+-/?@#$%^&*\" ,:"
        );

        List<String> alwaysInvalid = Lists.newArrayList(
                // inner column prefix
                "mv_",
                "mva_",
                "__doris_shadow_",

                // invalid
                "",
                "\\",
                "column\\",
                StringUtils.repeat("a", 257)
        );

        List<String> unicodeValid = Lists.newArrayList(
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
                    ExceptionChecker.expectThrows(AnalysisException.class, () -> validator.accept(s));
                }
                for (String s : unicodeValid) {
                    if (unicode) {
                        ExceptionChecker.expectThrowsNoException(() -> validator.accept(s));
                    } else {
                        ExceptionChecker.expectThrows(AnalysisException.class, () -> validator.accept(s));
                    }
                }
            }
        } finally {
            VariableMgr.getDefaultSessionVariable().setEnableUnicodeNameSupport(defaultUnicode);
        }
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
        String commonName = "test_sys_partition_list_basic_test_list_partition_bigint_tb-uniq";

        // check common name use correct regex, length 65
        ExceptionChecker.expectThrows(AnalysisException.class,
                () -> FeNameFormat.checkCommonName("fakeType", commonName + "t"));
        ExceptionChecker.expectThrows(AnalysisException.class,
                () -> FeNameFormat.checkCommonName("fakeType", "_commonName"));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkCommonName("fakeType", "a:b"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "common-Name"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "commonName-"));
    }

    @Test
    void testOutfileName() {
        // check success file name prefix
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkOutfileSuccessFileName("fakeType", "_success"));
    }
}
