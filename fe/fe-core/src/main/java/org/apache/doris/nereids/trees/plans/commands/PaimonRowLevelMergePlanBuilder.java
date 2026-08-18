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

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * MERGE INTO plan synthesizer for Paimon tables.
 *
 * <p>Differs from the Iceberg base in ONE spot: the matched-row anchor. Iceberg anchors on the row
 * locator, physically present on every scanned row. A paimon primary-key table's scan materializes
 * the locator as NULL (a merge-on-read task has no single backing data file), so anchoring on it
 * would classify every target row as NOT MATCHED — matched UPDATE degrades into an upsert-masked
 * insert and matched DELETE into a silent no-op. A primary-key column's VALUE is guaranteed
 * non-null on every existing row (and NULL exactly when the outer join found no match), so it is
 * the correct anchor. MERGE is gated to primary-key tables by the connector's row-level DML mode
 * check, so a key column always exists here.
 */
public class PaimonRowLevelMergePlanBuilder extends ExternalRowLevelMergePlanBuilder {

    public PaimonRowLevelMergePlanBuilder(
            List<String> targetNameParts,
            Optional<String> targetAlias,
            Optional<LogicalPlan> cte,
            LogicalPlan source,
            Expression onClause,
            List<MergeMatchedClause> matchedClauses,
            List<MergeNotMatchedClause> notMatchedClauses) {
        super(targetNameParts, targetAlias, cte, source, onClause, matchedClauses, notMatchedClauses);
    }

    @Override
    protected Expression buildMatchedAnchor(ExternalTable targetTable, Expression rowIdExpr,
            List<String> targetNameInPlan) {
        for (Column column : targetTable.getFullSchema()) {
            if (column.isKey()) {
                List<String> nameParts = Lists.newArrayList(targetNameInPlan);
                nameParts.add(column.getName());
                return new UnboundSlot(nameParts);
            }
        }
        // No key column (not reachable for a gated MERGE) — fall back to the base anchor.
        return rowIdExpr;
    }
}
