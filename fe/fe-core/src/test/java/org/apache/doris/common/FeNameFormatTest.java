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

        boolean defaultUnicode = VariableMgr.getDefaultSessionVariable().enableUnicodeNameSupport;
        List<Boolean> enableUnicode = Lists.newArrayList(false, true);
        try {
            for (Boolean unicode : enableUnicode) {
                VariableMgr.getDefaultSessionVariable().setEnableUnicodeNameSupport(unicode);
                for (String s : alwaysValid) {
                    ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName(s));
                }
                for (String s : alwaysInvalid) {
                    ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkColumnName(s));
                }
                for (String s : unicodeValid) {
                    if (unicode) {
                        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName(s));
                    } else {
                        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkColumnName(s));
                    }
                }
            }
        } finally {
            VariableMgr.getDefaultSessionVariable().setEnableUnicodeNameSupport(defaultUnicode);
        }
    }

    @Test
    void testUserName() {
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkUserName("a.b"));
        // check user name use correct regex, not begin with '.'
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkUserName(".a.b"));
    }

    @Test
    void testCommonName() {
        String commonName = "test_sys_partition_list_basic_test_list_partition_bigint_tb-uniq";

        // check common name use correct regex, length 65
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkCommonName("fakeType", commonName + "t"));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkCommonName("fakeType", "_commonName"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "common-Name"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "commonName-"));
    }

    @Test
    void testOutfileName() {
        // check success file name prefix
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkOutfileSuccessFileName("fakeType", "_success"));
    }
}
