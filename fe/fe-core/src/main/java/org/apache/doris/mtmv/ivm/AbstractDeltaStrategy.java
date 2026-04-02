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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Shared helpers for IVM delta rewrite strategies.
 *
 * <p>Provides common operations used by both scan-only and aggregate delta strategies:
 * stripping result sinks, extracting scan nodes, building insert commands, etc.
 */
public abstract class AbstractDeltaStrategy implements IvmDeltaStrategy {

    /** Name used for the mock dml_factor column (1 for insert, -1 for delete). */
    protected static final String DML_FACTOR_NAME = "__dml_factor__";

    /** Strips {@link LogicalResultSink} wrappers from the top of a plan tree. */
    protected static Plan stripResultSink(Plan plan) {
        while (plan instanceof LogicalResultSink) {
            plan = ((LogicalResultSink<?>) plan).child();
        }
        return plan;
    }

    /**
     * Walks down through Project / Aggregate nodes to find the leaf OlapScan.
     * Throws if an unsupported node type is encountered.
     */
    protected static LogicalOlapScan extractScan(Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return (LogicalOlapScan) plan;
        }
        if (plan instanceof LogicalProject) {
            return extractScan(((LogicalProject<?>) plan).child());
        }
        if (plan instanceof LogicalAggregate) {
            return extractScan(((LogicalAggregate<?>) plan).child());
        }
        throw new AnalysisException(
                "IVM delta rewrite does not yet support: " + plan.getClass().getSimpleName());
    }

    /** Builds a {@link BaseTableInfo} from a scan node. */
    protected static BaseTableInfo extractBaseTableInfo(LogicalOlapScan scan) {
        return new BaseTableInfo(scan.getTable(), 0L);
    }

    /**
     * Wraps a query plan with an {@link UnboundTableSink} and {@link InsertIntoTableCommand}
     * targeting the given MTMV.
     */
    protected static Command buildInsertCommand(Plan queryPlan, IvmDeltaRewriteContext ctx) {
        MTMV mtmv = ctx.getMtmv();
        List<String> mvNameParts = ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
        UnboundTableSink<LogicalPlan> sink = new UnboundTableSink<>(
                mvNameParts, mtmv.getInsertedColumnNames(), ImmutableList.of(),
                false, ImmutableList.of(), false,
                TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.INSERT,
                Optional.empty(), Optional.empty(), (LogicalPlan) queryPlan);
        return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Returns a new project that appends a mock {@code dml_factor = 1} column to the given
     * bottom project's output list. The original child plan is preserved.
     *
     * <p>The mock factor will be replaced with a real stream-sourced value once stream
     * integration is ready.
     */
    protected static LogicalProject<?> appendMockDmlFactor(LogicalProject<?> bottomProject) {
        List<NamedExpression> outputs = new ArrayList<>(bottomProject.getProjects());
        outputs.add(new Alias(new TinyIntLiteral((byte) 1), DML_FACTOR_NAME));
        return new LogicalProject<>(ImmutableList.copyOf(outputs), bottomProject.child());
    }
}
