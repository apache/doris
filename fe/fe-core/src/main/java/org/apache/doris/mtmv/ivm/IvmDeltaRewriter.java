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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Transforms a normalized MV plan into delta INSERT commands.
 *
 * <p>Supported patterns:
 * <ul>
 *   <li>SCAN_ONLY:    ResultSink → Project → OlapScan</li>
 *   <li>PROJECT_SCAN: ResultSink → Project → Project → OlapScan</li>
 * </ul>
 */
public class IvmDeltaRewriter {

    /**
     * Rewrites the normalized plan into a list of delta command bundles.
     * Currently produces exactly one INSERT bundle for the single base table scan.
     */
    public List<DeltaCommandBundle> rewrite(Plan normalizedPlan, IvmDeltaRewriteContext ctx) {
        Plan queryPlan = stripResultSink(normalizedPlan);
        LogicalOlapScan scan = validateAndExtractScan(queryPlan);
        BaseTableInfo baseTableInfo = new BaseTableInfo(scan.getTable(), 0L);
        Command insertCommand = buildInsertCommand(queryPlan, ctx);
        return Collections.singletonList(new DeltaCommandBundle(baseTableInfo, insertCommand));
    }

    private Plan stripResultSink(Plan plan) {
        if (plan instanceof LogicalResultSink) {
            return ((LogicalResultSink<?>) plan).child();
        }
        return plan;
    }

    private LogicalOlapScan validateAndExtractScan(Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return (LogicalOlapScan) plan;
        }
        if (plan instanceof LogicalProject) {
            return validateAndExtractScan(((LogicalProject<?>) plan).child());
        }
        throw new AnalysisException(
                "IVM delta rewrite does not yet support: " + plan.getClass().getSimpleName());
    }

    private Command buildInsertCommand(Plan queryPlan, IvmDeltaRewriteContext ctx) {
        MTMV mtmv = ctx.getMtmv();
        List<String> mvNameParts = ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
        UnboundTableSink<LogicalPlan> sink = new UnboundTableSink<>(
                mvNameParts, ImmutableList.of(), ImmutableList.of(),
                false, ImmutableList.of(), false,
                TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.INSERT,
                Optional.empty(), Optional.empty(), (LogicalPlan) queryPlan);
        return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty());
    }
}
