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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

public class ShowBackupCommandTest {
    @Test
    void testGetSnapshotPredicate() throws AnalysisException {
        String snapshotName = "mysql_edu_mall_mall_source_occupy_1741669209_1741669211";
        Predicate<String> predicate = CaseSensibility.LABEL.getCaseSensibility()
                    ? label -> label.equals(snapshotName) : label -> label.equalsIgnoreCase(snapshotName);
        Assertions.assertTrue(predicate.test(snapshotName));
    }

    @Test
    void testValidate() throws UserException {
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new VarcharLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921"));
        String leftKey = ((UnboundSlot) equalTo.child(0)).getName();
        Assertions.assertEquals("snapshotname", leftKey);

        Expression like = new Like(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new VarcharLiteral("mysql_edu_mall_%"));
        String snapshotNameLike = ((StringLikeLiteral) like.child(1)).getStringValue();
        Assertions.assertFalse(Strings.isNullOrEmpty(snapshotNameLike));
    }
}
