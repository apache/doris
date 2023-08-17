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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * insert into values command
 */
public class InsertIntoValuesCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final List<List<Literal>> values;
    private Database database;
    private OlapTable olapTable;
    private LogicalPlan query;

    public InsertIntoValuesCommand(List<String> nameParts, List<List<Literal>> values) {
        super(PlanType.INSERT_INTO_VALUES_COMMAND);
        this.nameParts = nameParts;
        this.values = Utils.copyRequiredList(values);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        bindTable(ctx);
        buildQuery();
        new InsertIntoTableCommand(query, null).run(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoValuesCommand(this, context);
    }

    private void bindTable(ConnectContext ctx) {
        List<String> tableQualifier = RelationUtil.getQualifierName(ctx, nameParts);
        Pair<DatabaseIf, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier, ctx.getEnv());
        if (!(pair.second instanceof OlapTable)) {
            throw new AnalysisException("the target table of insert into is not an OLAP table");
        }
        database = ((Database) pair.first);
        olapTable = ((OlapTable) pair.second);
    }

    private void buildQuery() {
        List<UnboundOneRowRelation> rows = values.stream().map(l ->
                        new UnboundOneRowRelation(
                                StatementScopeIdGenerator.newRelationId(),
                                l.stream().map(UnboundAlias::new).collect(Collectors.toList())))
                .collect(Collectors.toList());
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, rows);
        UnboundOlapTableSink sink = new UnboundOlapTableSink(
                nameParts,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                union);
        query = sink;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return Suppliers.memoize(() -> {
            buildQuery();
            return query;
        }).get();
    }
}
