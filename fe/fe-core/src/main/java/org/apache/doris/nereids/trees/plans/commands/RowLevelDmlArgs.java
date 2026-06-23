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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;
import java.util.Optional;

/**
 * Immutable carrier of the per-operation arguments a {@link RowLevelDmlTransform} needs to synthesize a
 * row-level DML plan, plus the already-resolved target {@link TableIf}.
 *
 * <p>The dispatching command ({@code UpdateCommand}/{@code DeleteFromCommand}/{@code MergeIntoCommand})
 * resolves the table (preserving its own swallow/throw discipline) and builds the matching variant via the
 * {@code forDelete}/{@code forUpdate}/{@code forMerge} factories. Fields are a union across the three
 * operations; only the relevant ones are populated per factory. {@code cte} is forwarded for MERGE only —
 * UPDATE/DELETE drop it, faithful to legacy {@code IcebergUpdateCommand}/{@code IcebergDeleteCommand}, which
 * carry no CTE.</p>
 */
public final class RowLevelDmlArgs {

    private final TableIf table;

    // DELETE / UPDATE
    private final List<String> nameParts;
    private final String tableAlias;
    private final LogicalPlan logicalQuery;
    private final DeleteCommandContext deleteCtx;
    // DELETE only
    private final boolean isTempPart;
    private final List<String> partitions;
    // UPDATE only
    private final List<EqualTo> assignments;

    // MERGE
    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final Optional<LogicalPlan> cte;
    private final LogicalPlan source;
    private final Expression onClause;
    private final List<MergeMatchedClause> matchedClauses;
    private final List<MergeNotMatchedClause> notMatchedClauses;

    private RowLevelDmlArgs(TableIf table, List<String> nameParts, String tableAlias, LogicalPlan logicalQuery,
            DeleteCommandContext deleteCtx, boolean isTempPart, List<String> partitions, List<EqualTo> assignments,
            List<String> targetNameParts, Optional<String> targetAlias, Optional<LogicalPlan> cte, LogicalPlan source,
            Expression onClause, List<MergeMatchedClause> matchedClauses,
            List<MergeNotMatchedClause> notMatchedClauses) {
        this.table = table;
        this.nameParts = nameParts;
        this.tableAlias = tableAlias;
        this.logicalQuery = logicalQuery;
        this.deleteCtx = deleteCtx;
        this.isTempPart = isTempPart;
        this.partitions = partitions;
        this.assignments = assignments;
        this.targetNameParts = targetNameParts;
        this.targetAlias = targetAlias;
        this.cte = cte;
        this.source = source;
        this.onClause = onClause;
        this.matchedClauses = matchedClauses;
        this.notMatchedClauses = notMatchedClauses;
    }

    /** Arguments for a DELETE (mirrors the legacy {@code IcebergDeleteCommand} constructor inputs). */
    public static RowLevelDmlArgs forDelete(TableIf table, List<String> nameParts, String tableAlias,
            boolean isTempPart, List<String> partitions, LogicalPlan logicalQuery, DeleteCommandContext deleteCtx) {
        return new RowLevelDmlArgs(table, nameParts, tableAlias, logicalQuery, deleteCtx, isTempPart, partitions,
                null, null, null, null, null, null, null, null);
    }

    /** Arguments for an UPDATE (mirrors the legacy {@code IcebergUpdateCommand} constructor inputs). */
    public static RowLevelDmlArgs forUpdate(TableIf table, List<String> nameParts, String tableAlias,
            List<EqualTo> assignments, LogicalPlan logicalQuery, DeleteCommandContext deleteCtx) {
        return new RowLevelDmlArgs(table, nameParts, tableAlias, logicalQuery, deleteCtx, false, null,
                assignments, null, null, null, null, null, null, null);
    }

    /** Arguments for a MERGE INTO (mirrors the legacy {@code IcebergMergeCommand} constructor inputs). */
    public static RowLevelDmlArgs forMerge(TableIf table, List<String> targetNameParts, Optional<String> targetAlias,
            Optional<LogicalPlan> cte, LogicalPlan source, Expression onClause,
            List<MergeMatchedClause> matchedClauses, List<MergeNotMatchedClause> notMatchedClauses) {
        return new RowLevelDmlArgs(table, null, null, null, null, false, null, null,
                targetNameParts, targetAlias, cte, source, onClause, matchedClauses, notMatchedClauses);
    }

    public TableIf getTable() {
        return table;
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    public DeleteCommandContext getDeleteCtx() {
        return deleteCtx;
    }

    public boolean isTempPart() {
        return isTempPart;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<EqualTo> getAssignments() {
        return assignments;
    }

    public List<String> getTargetNameParts() {
        return targetNameParts;
    }

    public Optional<String> getTargetAlias() {
        return targetAlias;
    }

    public Optional<LogicalPlan> getCte() {
        return cte;
    }

    public LogicalPlan getSource() {
        return source;
    }

    public Expression getOnClause() {
        return onClause;
    }

    public List<MergeMatchedClause> getMatchedClauses() {
        return matchedClauses;
    }

    public List<MergeNotMatchedClause> getNotMatchedClauses() {
        return notMatchedClauses;
    }
}
