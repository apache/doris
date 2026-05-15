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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Builds commands for one generated delta plan.
 */
class IvmDeltaCommandBuilder {
    static final IvmDeltaCommandBuilder INSTANCE = new IvmDeltaCommandBuilder();

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;
    private final IvmDeltaRewriteVisitor visitor;

    private IvmDeltaCommandBuilder() {
        visitor = new IvmDeltaRewriteVisitor();
    }

    List<Command> rewrite(Plan deltaPlan, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult result = visitor.rewritePlan(deltaPlan, ctx);
        Plan finalPlan;
        if (result.terminal) {
            finalPlan = result.plan;
        } else {
            finalPlan = helper.buildSinkProject(result, ctx);
        }
        return ImmutableList.of(buildCommandWithDeleteSign(finalPlan, ctx));
    }

    Command buildCommandWithDeleteSign(Plan queryPlan, IvmRefreshContext ctx) {
        MTMV mtmv = ctx.getMtmv();
        List<String> sinkColumns = new ArrayList<>(mtmv.getInsertedColumnNames());
        sinkColumns.add(Column.DELETE_SIGN);
        List<String> mvNameParts = ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
        UnboundTableSink<LogicalPlan> sink = new UnboundTableSink<>(
                mvNameParts, sinkColumns, ImmutableList.of(),
                false, ImmutableList.of(), false,
                TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.INSERT,
                Optional.empty(), Optional.empty(), (LogicalPlan) queryPlan);
        return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty());
    }
}
