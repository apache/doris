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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of an identifier whose text contains one of the delimiters of the canonical form. The text
 * based reader of HBO SET STATISTICS looks for scan tokens in the struct info it is given, so an
 * unescaped name which starts like one makes it truncate the text - and the statement then hashes the
 * unstripped struct info and rejects the fingerprint the user copied from EXPLAIN.
 */
public class GroupStructInfoIdentifierEscapeTest {

    @Test
    public void testAnIdentifierWhichLooksLikeATokenIsEscaped() {
        LogicalOlapScan t = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        SlotReference slot = new SlotReference(new ExprId(9), "a;S{b}", IntegerType.INSTANCE, true,
                ImmutableList.of("db", "t"));
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), t);
        GroupStructInfo info = GroupStructInfo.describeGroup(new Memo(null, filter).getRoot());
        Assertions.assertTrue(info.isValid(), info.getSkipReason());

        String canonical = info.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
        Assertions.assertTrue(canonical.contains("col(db.t.a%3BS%7Bb%7D)"), canonical);
        Assertions.assertFalse(canonical.contains("a;S{b}"), canonical);
        // what HBO SET STATISTICS hashes: stripping the baselines of the printed struct info has to give
        // the shape back, whatever the identifiers look like
        Assertions.assertEquals(info.getShape(GroupStructInfo.LiteralMode.WITH_LITERAL),
                GroupStructInfo.stripScanBaseline(canonical));
        Assertions.assertEquals("a;S{b}", GroupStructInfo.decodeLiteralValue("a%3BS%7Bb%7D"));
    }

    @Test
    public void testAnIdentifierWithACommaInsideAFakeTokenIsEscaped() {
        // `;` is a boundary the reader accepts in front of a scan token and the comma splits the baseline
        // of one: a name of `a;S{b,v2}` would be rewritten to `S{b}` by the stripper, so the struct info a
        // user pasted would hash to something else than the fingerprint EXPLAIN printed
        LogicalOlapScan t = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        SlotReference slot = new SlotReference(new ExprId(10), "a;S{b,v2}", IntegerType.INSTANCE, true,
                ImmutableList.of("db", "t"));
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), t);
        GroupStructInfo info = GroupStructInfo.describeGroup(new Memo(null, filter).getRoot());
        Assertions.assertTrue(info.isValid(), info.getSkipReason());
        String canonical = info.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
        Assertions.assertTrue(canonical.contains("col(db.t.a%3BS%7Bb%2Cv2%7D)"), canonical);
        Assertions.assertEquals(info.getShape(GroupStructInfo.LiteralMode.WITH_LITERAL),
                GroupStructInfo.stripScanBaseline(canonical));
    }
}
