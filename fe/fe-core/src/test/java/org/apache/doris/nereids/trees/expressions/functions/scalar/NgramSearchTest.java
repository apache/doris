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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NgramSearchTest {

    @Test
    void testLiteralGramNumber() {
        assertFoldableGramNumber(new IntegerLiteral(3));
    }

    @Test
    void testFoldableArithmeticGramNumber() {
        assertFoldableGramNumber(new Add(new IntegerLiteral(1), new IntegerLiteral(2)));
    }

    @Test
    void testFoldableCastGramNumber() {
        assertFoldableGramNumber(new Cast(new StringLiteral("3"), IntegerType.INSTANCE));
    }

    @Test
    void testFoldableFunctionGramNumber() {
        assertFoldableGramNumber(new Abs(new IntegerLiteral(-3)));
    }

    @Test
    void testConstantGramNumberBeyondFeEvaluator() {
        // FE cannot evaluate crc32, so the constant stays unfolded and is left for BE to
        // evaluate and validate; FE must not reject it as nonconstant at either stage.
        Expression gram = new Add(new Mod(new Crc32(new StringLiteral("abc")), new IntegerLiteral(3)),
                new IntegerLiteral(1));
        NgramSearch analyzed = analyze(gram);
        Assertions.assertFalse(analyzed.child(2).isLiteral());
        Assertions.assertDoesNotThrow(analyzed::checkLegalityAfterRewrite);
    }

    @Test
    void testAfterRewriteRejectsInvalidLiteral() {
        NgramSearch analyzed = analyze(new IntegerLiteral(3));
        assertAfterRewriteFails(withGramNumber(analyzed, new IntegerLiteral(0)),
                "gram_num must be a positive constant");
        assertAfterRewriteFails(withGramNumber(analyzed, new IntegerLiteral(-1)),
                "gram_num must be a positive constant");
        assertAfterRewriteFails(withGramNumber(analyzed, new NullLiteral(IntegerType.INSTANCE)),
                "gram_num support const value only");
    }

    @Test
    void testNonPositiveGramNumber() {
        assertAnalyzeFails(new IntegerLiteral(0), "gram_num must be a positive constant");
        assertAnalyzeFails(new IntegerLiteral(-1), "gram_num must be a positive constant");
        assertAnalyzeFails(new Subtract(new IntegerLiteral(1), new IntegerLiteral(1)),
                "gram_num must be a positive constant");
        assertAnalyzeFails(new Subtract(new IntegerLiteral(1), new IntegerLiteral(2)),
                "gram_num must be a positive constant");
        assertAnalyzeFails(new Cast(new StringLiteral("0"), IntegerType.INSTANCE),
                "gram_num must be a positive constant");
    }

    @Test
    void testNonConstantGramNumber() {
        SlotReference gram = SlotReference.of("gram", IntegerType.INSTANCE);
        assertAnalyzeFails(gram, "gram_num support const value only");
        assertAnalyzeFails(new Add(gram, new IntegerLiteral(1)), "gram_num support const value only");
        assertAnalyzeFails(new Cast(new Random(), IntegerType.INSTANCE), "gram_num support const value only");
    }

    @Test
    void testNonIntegerGramNumber() {
        assertAnalyzeFails(new StringLiteral("3"), "gram_num support const value only");
        assertAnalyzeFails(new DoubleLiteral(3.0), "gram_num support const value only");
        assertAnalyzeFails(new NullLiteral(), "gram_num support const value only");
        assertAnalyzeFails(new Cast(new NullLiteral(), IntegerType.INSTANCE), "gram_num support const value only");
    }

    @Test
    void testNonConstantPattern() {
        NgramSearch function = new NgramSearch(new StringLiteral("abc"),
                SlotReference.of("pattern", StringType.INSTANCE), new IntegerLiteral(3));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ExpressionAnalyzer.analyzeFunction(null, null, function));
        Assertions.assertTrue(exception.getMessage().contains("pattern support const value only"));
    }

    private NgramSearch analyze(Expression gram) {
        Expression analyzed = ExpressionAnalyzer.analyzeFunction(null, null,
                new NgramSearch(new StringLiteral("abc"), new StringLiteral("abc"), gram));
        return (NgramSearch) analyzed;
    }

    private NgramSearch withGramNumber(NgramSearch function, Expression gram) {
        return function.withChildren(ImmutableList.of(function.child(0), function.child(1), gram));
    }

    private void assertFoldableGramNumber(Expression gram) {
        NgramSearch analyzed = analyze(gram);
        Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(analyzed);
        Assertions.assertEquals(new IntegerLiteral(3), folded.child(2));
        Assertions.assertDoesNotThrow(folded::checkLegalityAfterRewrite);
    }

    private void assertAnalyzeFails(Expression gram, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> analyze(gram));
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private void assertAfterRewriteFails(NgramSearch function, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }
}
