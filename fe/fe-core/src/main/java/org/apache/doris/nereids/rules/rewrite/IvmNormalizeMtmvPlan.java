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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.ivm.IvmContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Normalizes the MV define plan for IVM at both CREATE MV and REFRESH MV time.
 * - Injects __IVM_ROW_ID__ at index 0 of each OlapScan output via a wrapping LogicalProject:
 *   - MOW (UNIQUE_KEYS + merge-on-write): Alias(cast(murmur_hash3_64(uk...) as LargeInt), "__IVM_ROW_ID__")
 *     → deterministic (stable across refreshes)
 *   - DUP_KEYS: Alias(uuid_numeric(), "__IVM_ROW_ID__") → non-deterministic (random per insert)
 *   - Other key types: not supported, throws.
 * - Records (rowIdSlot → isDeterministic) in IvmContext on CascadesContext.
 * - visitLogicalProject propagates child's row-id slot if not already in outputs.
 * - Whitelists supported plan nodes; throws AnalysisException for unsupported nodes.
 * Supported: OlapScan, project.
 * TODO: avg rewrite, join support.
 */
public class IvmNormalizeMtmvPlan extends DefaultPlanRewriter<IvmContext> implements CustomRewriter {

    public static final String IVM_ROW_ID_COL = "__IVM_ROW_ID__";

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableIvmNormalRewrite()) {
            return plan;
        }
        IvmContext ivmContext = new IvmContext();
        jobContext.getCascadesContext().setIvmContext(ivmContext);
        return plan.accept(this, ivmContext);
    }

    // unsupported: any plan node not explicitly whitelisted below
    @Override
    public Plan visit(Plan plan, IvmContext ivmContext) {
        throw new AnalysisException("IVM does not support plan node: "
                + plan.getClass().getSimpleName());
    }

    // whitelisted: only OlapScan — inject IVM row-id at index 0
    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, IvmContext ivmContext) {
        OlapTable table = scan.getTable();
        Pair<Expression, Boolean> rowId = buildRowId(table, scan);
        Alias rowIdAlias = new Alias(rowId.first, IVM_ROW_ID_COL);
        ivmContext.addRowId(rowIdAlias.toSlot(), rowId.second);
        List<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .add(rowIdAlias)
                .addAll(scan.getOutput())
                .build();
        return new LogicalProject<>(outputs, scan);
    }

    // whitelisted: project — recurse into child, then propagate row-id if not already present
    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, IvmContext ivmContext) {
        Plan newChild = project.child().accept(this, ivmContext);
        // find the row-id slot from the child's output (always at index 0 after normalization)
        Slot childRowId = newChild.getOutput().stream()
                .filter(s -> IVM_ROW_ID_COL.equals(s.getName()))
                .findFirst()
                .orElseThrow(() -> new AnalysisException(
                        "IVM normalization error: child plan has no row-id slot after normalization"));
        boolean hasRowId = project.getProjects().stream()
                .anyMatch(e -> e instanceof Slot && IVM_ROW_ID_COL.equals(((Slot) e).getName()));
        if (hasRowId) {
            return newChild == project.child() ? project : project.withChildren(ImmutableList.of(newChild));
        }
        // prepend child's row-id slot to this project's outputs
        List<NamedExpression> newOutputs = ImmutableList.<NamedExpression>builder()
                .add(childRowId)
                .addAll(project.getProjects())
                .build();
        return new LogicalProject<>(newOutputs, newChild);
    }

    /**
     * Builds the row-id expression and returns whether it is deterministic as a pair.
     * - MOW: (buildRowIdHash(uk...), true)  — stable across refreshes
     * - DUP_KEYS: (UuidNumeric(), false)    — random per insert
     * - Other key types: throws AnalysisException
     */
    private Pair<Expression, Boolean> buildRowId(OlapTable table, LogicalOlapScan scan) {
        KeysType keysType = table.getKeysType();
        if (keysType == KeysType.UNIQUE_KEYS && table.getEnableUniqueKeyMergeOnWrite()) {
            List<String> keyColNames = table.getBaseSchemaKeyColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
            List<Expression> keySlots = scan.getOutput().stream()
                    .filter(s -> keyColNames.contains(s.getName()))
                    .collect(Collectors.toList());
            if (keySlots.isEmpty()) {
                throw new AnalysisException("IVM: no unique key columns found for MOW table: "
                        + table.getName());
            }
            return Pair.of(buildRowIdHash(keySlots), true);
        }
        if (keysType == KeysType.DUP_KEYS) {
            return Pair.of(new UuidNumeric(), false);
        }
        throw new AnalysisException("IVM does not support table key type: " + keysType
                + " for table: " + table.getName()
                + ". Only MOW (UNIQUE_KEYS with merge-on-write) and DUP_KEYS are supported.");
    }

    /**
     * Builds a hash expression over the given key slots for use as a deterministic row-id.
     * Currently uses murmur_hash3_64 (64-bit) which is not collision-safe for large tables.
     * TODO: replace with a 128-bit hash once BE supports it or a Java UDF is available.
     */
    private Expression buildRowIdHash(List<Expression> keySlots) {
        Expression first = keySlots.get(0);
        Expression[] rest = keySlots.subList(1, keySlots.size()).toArray(new Expression[0]);
        return new MurmurHash364(first, rest);
    }
}
