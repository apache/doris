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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConcatWs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitByString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitPart;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link ExpressionUtils#canEvaluateOnDictionary(Expression)}. Verifies that
 * only NULL-preserving (null-in implies null-out), single-slot, deterministic value sides are
 * marked dict-filterable, so a NULL row -- which has no dictionary entry -- can never be wrongly
 * dropped or kept when the predicate is rewritten into a dict-code filter.
 */
public class DictFilterEvaluationTest {

    private final SlotReference col = new SlotReference("col", VarcharType.SYSTEM_DEFAULT);
    private final SlotReference other = new SlotReference("other", VarcharType.SYSTEM_DEFAULT);

    private static boolean canEvaluateOnDictionary(Expression conjunct) {
        return ExpressionUtils.canEvaluateOnDictionary(conjunct);
    }

    private static EqualTo eq(Expression valueSide) {
        return new EqualTo(valueSide, new VarcharLiteral("x"));
    }

    // element_at(split_by_string(col, ','), 1) : the core target of the optimization.
    private ElementAt splitElement() {
        SplitByString split = new SplitByString(col, new VarcharLiteral(","));
        return new ElementAt(split, new BigIntLiteral(1L));
    }

    @Test
    public void bareSlotAccepted() {
        Assertions.assertTrue(canEvaluateOnDictionary(eq(col)));
    }

    @Test
    public void splitElementAccepted() {
        Assertions.assertTrue(canEvaluateOnDictionary(eq(splitElement())));
    }

    @Test
    public void substrAccepted() {
        Substring substr = new Substring(col, new IntegerLiteral(1), new IntegerLiteral(3));
        Assertions.assertTrue(canEvaluateOnDictionary(eq(substr)));
    }

    @Test
    public void splitPartAccepted() {
        SplitPart splitPart = new SplitPart(col, new VarcharLiteral(","), new IntegerLiteral(1));
        Assertions.assertTrue(canEvaluateOnDictionary(eq(splitPart)));
    }

    @Test
    public void regexpExtractAccepted() {
        RegexpExtract regexp = new RegexpExtract(col, new VarcharLiteral("(\\d+)"), new IntegerLiteral(1));
        Assertions.assertTrue(canEvaluateOnDictionary(eq(regexp)));
    }

    @Test
    public void coalesceRejected() {
        // coalesce(col, 'N') = 'N' turns a NULL row into 'N', which the dictionary (no NULL
        // entry) cannot represent -> NULL rows would be wrongly dropped.
        Coalesce coalesce = new Coalesce(col, new VarcharLiteral("N"));
        Assertions.assertFalse(canEvaluateOnDictionary(eq(coalesce)));
    }

    @Test
    public void ifnullRejected() {
        Nvl ifnull = new Nvl(col, new VarcharLiteral("N"));
        Assertions.assertFalse(canEvaluateOnDictionary(eq(ifnull)));
    }

    @Test
    public void ifRejected() {
        If ifExpr = new If(new IsNull(col), new VarcharLiteral("N"), col);
        Assertions.assertFalse(canEvaluateOnDictionary(eq(ifExpr)));
    }

    @Test
    public void concatWsRejected() {
        // concat_ws skips NULL args (concat_ws(',', NULL) = ''), so it is not null-in-null-out.
        ConcatWs concatWs = new ConcatWs(new VarcharLiteral(","), col);
        Assertions.assertFalse(canEvaluateOnDictionary(eq(concatWs)));
    }

    @Test
    public void multiSlotRejected() {
        // concat(col, other) is null-preserving but references two columns, so it cannot be
        // evaluated on a single column's dictionary -- rejected by the single-slot check.
        Concat multi = new Concat(col, other);
        Assertions.assertFalse(canEvaluateOnDictionary(eq(multi)));
    }

    @Test
    public void nonEqualityRejected() {
        // Only equality / IN are rewritten into a dict-code predicate; a comparison to a
        // non-literal is not a candidate.
        Assertions.assertFalse(canEvaluateOnDictionary(new EqualTo(col, other)));
    }
}
