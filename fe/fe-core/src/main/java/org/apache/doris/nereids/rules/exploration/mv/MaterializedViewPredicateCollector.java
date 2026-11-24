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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.rewrite.PullUpPredicates;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Derive predicates for MV matching using guarantees on materialized table outputs.
 *
 * <p>The definition cache owns the guarantees; each visit binds them to this scan's actual slots.
 * No expression state is attached to a scan, so ordinary scans, replacement scans and deep copies
 * follow the same path. PullUpPredicates retains the operator boundaries, including null extension
 * by outer joins and columns removed by projections or aggregation.
 */
public final class MaterializedViewPredicateCollector extends PullUpPredicates {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewPredicateCollector.class);

    private final CascadesContext cascadesContext;

    public MaterializedViewPredicateCollector(CascadesContext cascadesContext) {
        super(true, cascadesContext);
        this.cascadesContext = cascadesContext;
    }

    /** Derive guarantees on a filter's input for the current MV matching analysis. */
    public Set<Expression> collect(Plan plan) {
        return plan.accept(this, null).stream()
                .filter(predicate -> !predicate.containsVolatileExpression())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public ImmutableSet<Expression> visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
        return cacheOrElse(scan, () -> {
            if (!(scan.getTable() instanceof MTMV)
                    || scan.getSelectedIndexId() != scan.getTable().getBaseIndexId()) {
                return ImmutableSet.of();
            }
            MTMV mtmv = (MTMV) scan.getTable();
            MTMVCache cache;
            try {
                cache = mtmv.getOrGenerateCache(cascadesContext.getConnectContext());
            } catch (Exception exception) {
                // A stored MV remains readable when its definition can no longer be planned.
                // As with MV data-trait derivation, unavailable metadata supplies no rewrite proof.
                LOG.warn("Cannot derive output predicates for materialized view {}", mtmv.getName(), exception);
                return ImmutableSet.of();
            }
            if (cache.getOutputPredicates().isEmpty()) {
                return ImmutableSet.of();
            }
            return ImmutableSet.copyOf(ExpressionUtils.replace(cache.getOutputPredicates(),
                    mapDefinitionOutputs(cache, scan)));
        });
    }

    private static Map<Slot, Slot> mapDefinitionOutputs(MTMVCache cache, LogicalOlapScan scan) {
        List<Slot> definitionOutputs = cache.getOriginalFinalPlan().getOutput().stream()
                .filter(slot -> slot instanceof SlotReference && ((SlotReference) slot).isVisible())
                .collect(Collectors.toList());
        List<Column> physicalColumns = scan.getTable().getBaseSchema(false);
        Preconditions.checkState(definitionOutputs.size() == physicalColumns.size(),
                "materialized view definition output size %s does not match visible schema size %s",
                definitionOutputs.size(), physicalColumns.size());

        Map<String, Slot> scanColumns = new HashMap<>();
        for (Slot slot : scan.getOutput()) {
            if (slot instanceof SlotReference) {
                SlotReference reference = (SlotReference) slot;
                // Hidden IVM columns, derived variant paths and virtual columns are not definition outputs.
                if (reference.isVisible() && reference.getSubPath().isEmpty()) {
                    reference.getOriginalColumn().ifPresent(column ->
                            scanColumns.put(column.getName().toLowerCase(Locale.ROOT), reference));
                }
            }
        }
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (int i = 0; i < physicalColumns.size(); i++) {
            String columnName = physicalColumns.get(i).getName();
            Slot scanSlot = Preconditions.checkNotNull(scanColumns.get(columnName.toLowerCase(Locale.ROOT)),
                    "materialized view scan does not output physical column %s", columnName);
            // Repeated definition outputs need one representative, not all combinations of scan slots.
            outputMapping.putIfAbsent(definitionOutputs.get(i), scanSlot);
        }
        return outputMapping;
    }
}
