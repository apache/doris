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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EncodeDecodePlannerTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Test
    void testLiteralCallsFoldDuringPlanRewrite() {
        VarBinaryLiteral encoded = new VarBinaryLiteral(new byte[] {0x4E, 0x2D});
        PlanChecker.from(connectContext)
                .analyze("select encode('中', 'UTF-16BE')")
                .rewrite()
                .matches(logicalResultSink(
                        logicalOneRowRelation().when(oneRow ->
                                oneRow.getProjects().get(0).child(0).equals(encoded))));

        StringLiteral decoded = new StringLiteral("中");
        PlanChecker.from(connectContext)
                .analyze("select decode(X'E4B8AD', 'UTF-8')")
                .rewrite()
                .matches(logicalResultSink(
                        logicalOneRowRelation().when(oneRow ->
                                oneRow.getProjects().get(0).child(0).equals(decoded))));
    }

    @Test
    void testInvalidCharsetRejectedBeforeNullFolding() {
        AnalysisException encodeError = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze("select encode(NULL, 'GBK')"));
        Assertions.assertTrue(encodeError.getMessage().contains("Unsupported character set"));

        AnalysisException decodeError = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze("select decode(NULL, 'GBK')"));
        Assertions.assertTrue(decodeError.getMessage().contains("Unsupported character set"));
    }
}
