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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class ExecuteCommandTest {

    @Test
    void testReapplySetVarHintForCachedExecution() throws Exception {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setTimeZone("+00:00");
        LogicalPlan plan = new LogicalSelectHint<>(
                ImmutableList.of(new SelectHintSetVar("SET_VAR",
                        ImmutableMap.of(SessionVariable.TIME_ZONE, Optional.of("+8:00")))),
                new UnboundOneRowRelation(new RelationId(1), ImmutableList.of()));

        StatementContext firstExecution = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        ExecuteCommand.applySetVarHints(plan, firstExecution);
        Assertions.assertEquals("+08:00", sessionVariable.getTimeZone());
        Assertions.assertEquals("+08:00", firstExecution.getStatementTimeZone().getId());

        VariableMgr.revertSessionValue(sessionVariable);
        sessionVariable.clearSessionOriginValue();
        sessionVariable.setIsSingleSetVar(false);
        Assertions.assertEquals("+00:00", sessionVariable.getTimeZone());

        StatementContext cachedExecution = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        ExecuteCommand.applySetVarHints(plan, cachedExecution);
        Assertions.assertEquals("+08:00", sessionVariable.getTimeZone());
        Assertions.assertEquals("+08:00", cachedExecution.getStatementTimeZone().getId());
    }
}
