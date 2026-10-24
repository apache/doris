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
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

class DataSketchesHllDownsampleTest {
    private static final Expression SKETCH = SlotReference.of("sk", StringType.INSTANCE);

    private DataSketchesHllDownsample analyze(Expression sketch, Expression k, Expression type) {
        return (DataSketchesHllDownsample) ExpressionAnalyzer.analyzeFunction(null, null,
                type == null ? new DataSketchesHllDownsample(sketch, k)
                        : new DataSketchesHllDownsample(sketch, k, type));
    }

    @Test
    void testInputTypesAndArity() {
        for (DataType type : ImmutableList.of(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT,
                VarBinaryType.INSTANCE)) {
            Expression input = SlotReference.of("sk", type);
            Assertions.assertEquals(type, analyze(input, new IntegerLiteral(12), null).getDataType());
            DataSketchesHllDownsample fn = analyze(input, new IntegerLiteral(12), new StringLiteral("HLL_4"));
            Assertions.assertEquals(type, fn.getDataType());
            Assertions.assertEquals(fn.getArguments(), fn.withChildren(fn.getArguments()).getArguments());
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze(new IntegerLiteral(1), new IntegerLiteral(12), null));
    }

    @Test
    void testBoundsAndTypes() {
        for (int k : new int[] {4, 7, 10, 15, 21}) {
            for (String type : ImmutableList.of("HLL_4", "hll_6", "HlL_8")) {
                Assertions.assertDoesNotThrow(() -> analyze(SKETCH, new IntegerLiteral(k), new StringLiteral(type)));
            }
        }
        for (Expression invalid : ImmutableList.of(new IntegerLiteral(3), new IntegerLiteral(22),
                new LargeIntLiteral(new BigInteger("18446744073709551628")),
                new StringLiteral("12"), new NullLiteral(IntegerType.INSTANCE),
                SlotReference.of("k", IntegerType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class, () -> analyze(SKETCH, invalid, null));
            // Invalid parameters must be rejected before NULL propagation can remove the call.
            Assertions.assertThrows(AnalysisException.class, () -> analyze(new NullLiteral(), invalid, null));
        }
        for (Expression invalid : ImmutableList.of(new StringLiteral("HLL_5"), new IntegerLiteral(4),
                new NullLiteral(StringType.INSTANCE), SlotReference.of("type", StringType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> analyze(SKETCH, new IntegerLiteral(12), invalid));
        }
    }

    @Test
    void testConstantExpressionsAndRewriteValidation() {
        DataSketchesHllDownsample fn = analyze(SKETCH,
                new Add(new IntegerLiteral(10), new IntegerLiteral(2)),
                new Concat(new StringLiteral("HLL_"), new StringLiteral("4")));
        Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(fn);
        Assertions.assertDoesNotThrow(folded::checkLegalityAfterRewrite);
        Assertions.assertEquals(new IntegerLiteral(12), folded.child(1));
        Expression onlyBe = new Add(new IntegerLiteral(10), new Crc32(new StringLiteral("")));
        Assertions.assertDoesNotThrow(() -> analyze(SKETCH, onlyBe, null).checkLegalityAfterRewrite());
        Expression nullCall = analyze(new NullLiteral(), onlyBe, null);
        Assertions.assertInstanceOf(DataSketchesHllDownsample.class,
                FoldConstantRuleOnFE.evaluateWithoutContext(nullCall));
        DataSketchesHllDownsample invalid = fn.withChildren(
                ImmutableList.of(SKETCH, new IntegerLiteral(30), new StringLiteral("HLL_4")));
        Assertions.assertThrows(AnalysisException.class, invalid::checkLegalityAfterRewrite);
        DataSketchesHllDownsample invalidType = fn.withChildren(
                ImmutableList.of(SKETCH, new IntegerLiteral(12), new StringLiteral("bad")));
        Assertions.assertThrows(AnalysisException.class, invalidType::checkLegalityAfterRewrite);
    }
}
