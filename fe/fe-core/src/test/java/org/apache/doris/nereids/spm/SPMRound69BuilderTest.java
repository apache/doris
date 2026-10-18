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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-69 review fix on the frozen-SQL expression printer:
 *
 * AVG_DISTINCT_TO_SUM_DIV_COUNT lowers `avg(DISTINCT k)` into
 * `sum(DISTINCT CAST(k AS LARGEINT)) / count(...)`, and that widening IS the LARGEINT
 * accumulator AVG computes in. The printer stripped EVERY implicit cast, so the frozen SQL
 * re-analyzed `SUM(DISTINCT k)` with a BIGINT accumulator: two distinct values 5e18 / 6e18 -
 * averaged to 5.5e18 by the original AVG - overflowed the replayed sum for the identical SQL
 * (the CREATE probe only re-plans the text, it never executes it).
 */
public class SPMRound69BuilderTest {

    private static String print(org.apache.doris.nereids.trees.expressions.Expression expr) {
        return new SPMExprSqlBuilder().print(expr, new SQLRelation());
    }

    /** The accumulator widening of a real column must survive into the frozen SQL. */
    @Test
    public void testAccumulatorWideningOverAColumnIsPreserved() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Cast widening = new Cast(k, LargeIntType.INSTANCE);

        Assertions.assertFalse(widening.isExplicitType(),
                "precondition: the AVG lowering inserts an ANALYZER cast");
        Assertions.assertEquals("CAST(k AS LARGEINT)", print(widening),
                "stripping the widening made the frozen SUM accumulate in BIGINT and"
                        + " overflow on two 5e18-scale distinct values");
    }

    /**
     * A cast over a SUBSTITUTED placeholder is the type pinning the stripping exists to
     * avoid: the captured literal's width is not a contract for the user's value.
     */
    @Test
    public void testWideningOverAPlaceholderIsStillStripped() {
        SpmConstVar placeholder = SpmConstVar.of(1L, new BigIntLiteral(5L));
        Cast widening = new Cast(placeholder, LargeIntType.INSTANCE);

        String printed = print(widening);
        Assertions.assertFalse(printed.contains("CAST"),
                "the frozen SQL must keep the bare placeholder - no captured-type pinning may"
                        + " leak into it: " + printed);
        Assertions.assertEquals("_spm_const_var(1)", printed,
                "the marker is rendered by id only: " + printed);
    }

    /** A narrowing / same-width implicit cast carries no accumulator guarantee. */
    @Test
    public void testNonWideningImplicitCastIsStillStripped() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);

        Assertions.assertEquals("k", print(new Cast(k, IntegerType.INSTANCE)),
                "a narrowing analyzer cast stays an execution detail");
        Assertions.assertEquals("k", print(new Cast(k, BigIntType.INSTANCE)),
                "a same-width analyzer cast stays an execution detail");
    }

    /**
     * An ordinary arithmetic widening (INT -> BIGINT) is NOT an accumulator: keeping it
     * would only pin coercions the re-analyzed user expression derives by itself.
     */
    @Test
    public void testArithmeticWideningIsStillStripped() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);

        Assertions.assertEquals("k", print(new Cast(k, BigIntType.INSTANCE)),
                "only the LARGEINT accumulator widening survives the freeze");
    }

    /** The user's own explicit cast keeps being rendered verbatim. */
    @Test
    public void testExplicitCastIsStillRendered() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        Cast explicit = new Cast(k, BigIntType.INSTANCE, true);

        Assertions.assertEquals("CAST(k AS BIGINT)", print(explicit));
    }
}
