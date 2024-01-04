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

import org.junit.Test;

public class FeNameFormatTest {

    @Test
    public void testCheckColumnName() {
        // check label use correct regex, begin with '-' is different from others
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkLabel("-lable"));

        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("_id"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("__id"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("___id"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("___id_"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("@timestamp"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("@timestamp#"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("timestamp*"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("timestamp.1"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("timestamp.#"));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkColumnName("?id_"));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkColumnName("#id_"));
        // length 64
        String tblName = "test_sys_partition_list_basic_test_list_partition_bigint_tb-uniq";
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkTableName(tblName));
        // length 70
        String largeTblName = "test_sys_partition_list_basic_test_list_partition_bigint_tb_uniq_large";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkTableName(largeTblName));
        // check table name use correct regex, not begin with '-'
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkTableName("-" + tblName));

        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkUserName("a.b"));
        // check user name use correct regex, not begin with '.'
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkUserName(".a.b"));

        // check common name use correct regex, length 65
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkCommonName("fakeType", tblName + "t"));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> FeNameFormat.checkCommonName("fakeType", "_commonName"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "common-Name"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkCommonName("fakeType", "commonName-"));

        // check success file name prefix
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkOutfileSuccessFileName("fakeType", "_success"));
    }

}
