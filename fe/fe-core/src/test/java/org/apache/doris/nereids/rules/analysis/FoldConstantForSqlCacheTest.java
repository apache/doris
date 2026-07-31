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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FoldConstantForSqlCacheTest {

    @Test
    void testUnfoldedNowDisablesSqlCacheOnEveryExecution() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        for (int execution = 0; execution < 2; execution++) {
            StatementContext statementContext = new StatementContext(
                    connectContext, new OriginStatement("select now(kint)", 0));
            connectContext.setStatementContext(statementContext);
            CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                    statementContext,
                    new UnboundRelation(new RelationId(execution + 1), ImmutableList.of("tbl")));
            ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(cascadesContext);
            SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().orElseThrow();
            Expression now = new Now(new SlotReference("kint", IntegerType.INSTANCE));

            Expression folded = FoldConstantForSqlCache.foldNondeterministic(
                    now, rewriteContext, sqlCacheContext);

            Assertions.assertSame(now, folded);
            Assertions.assertTrue(sqlCacheContext.containsCannotProcessExpression());
        }
    }
}
