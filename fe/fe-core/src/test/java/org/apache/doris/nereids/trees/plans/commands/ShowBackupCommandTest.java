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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

public class ShowBackupCommandTest {
    private final ExprId exprId = new ExprId(1);
    private final Expression left1 = new SlotReference(exprId, "left",
            IntegerType.INSTANCE, false, Lists.newArrayList());
    private final Expression right1 = new SlotReference(exprId, "right",
            IntegerType.INSTANCE, false, Lists.newArrayList());

    @Test
    void testGetSnapshotPredicate() throws AnalysisException {
        String snapshotName = "ccrs_mysql_edu_mall_mall_source_occupy_1741669209_1741669211";
        Predicate<String> predicate = CaseSensibility.LABEL.getCaseSensibility()
                    ? label -> label.equals(snapshotName) : label -> label.equalsIgnoreCase(snapshotName);

        Assertions.assertTrue(predicate.test("false"));
    }

    @Test
    void testValidate() throws UserException {
        Like like = new Like(left1, right1);
        EqualTo equalTo = new EqualTo(left1, right1);
        Assertions.assertTrue(like.child(0) instanceof UnboundSlot);

        String leftKey = ((UnboundSlot) like.child(0)).getName();
        Assertions.assertEquals("left", leftKey);

        String rightName = ((StringLikeLiteral) equalTo.child(1)).getStringValue();
        Assertions.assertEquals("right", rightName);
    }
}
