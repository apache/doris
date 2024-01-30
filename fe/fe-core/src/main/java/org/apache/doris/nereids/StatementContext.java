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

package org.apache.doris.nereids;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.analysis.ColumnAliasGenerator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;

/**
 * Statement context for nereids
 */
public class StatementContext {

    private ConnectContext connectContext;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    @GuardedBy("this")
    private final Map<String, Supplier<Object>> contextCacheMap = Maps.newLinkedHashMap();

    private OriginStatement originStatement;
    // NOTICE: we set the plan parsed by DorisParser to parsedStatement and if the plan is command, create a
    // LogicalPlanAdapter with the logical plan in the command.
    private StatementBase parsedStatement;
    private ColumnAliasGenerator columnAliasGenerator;

    private int joinCount = 0;
    private int maxNAryInnerJoin = 0;

    private boolean isDpHyp = false;

    // hasUnknownColStats true if any column stats in the tables used by this sql is unknown
    // the algorithm to derive plan when column stats are unknown is implemented in cascading framework, not in dphyper.
    // And hence, when column stats are unknown, even if the tables used by a sql is more than
    // MAX_TABLE_COUNT_USE_CASCADES_JOIN_REORDER, join reorder should choose cascading framework.
    // Thus hasUnknownColStats has higher priority than isDpHyp
    private boolean hasUnknownColStats = false;

    private final IdGenerator<ExprId> exprIdGenerator = ExprId.createGenerator();
    private final IdGenerator<ObjectId> objectIdGenerator = ObjectId.createGenerator();
    private final IdGenerator<RelationId> relationIdGenerator = RelationId.createGenerator();
    private final IdGenerator<CTEId> cteIdGenerator = CTEId.createGenerator();

    private final Map<CTEId, Set<LogicalCTEConsumer>> cteIdToConsumers = new HashMap<>();
    private final Map<CTEId, Set<NamedExpression>> cteIdToProjects = new HashMap<>();
    private final Map<RelationId, Set<Expression>> consumerIdToFilters = new HashMap<>();
    private final Map<CTEId, Set<RelationId>> cteIdToConsumerUnderProjects = new HashMap<>();
    // Used to update consumer's stats
    private final Map<CTEId, List<Pair<Map<Slot, Slot>, Group>>> cteIdToConsumerGroup = new HashMap<>();
    private final Map<CTEId, LogicalPlan> rewrittenCteProducer = new HashMap<>();
    private final Map<CTEId, LogicalPlan> rewrittenCteConsumer = new HashMap<>();
    private final Set<String> viewDdlSqlSet = Sets.newHashSet();

    // collect all hash join conditions to compute node connectivity in join graph
    private final List<Expression> joinFilters = new ArrayList<>();

    private final List<Hint> hints = new ArrayList<>();
    // Root Slot -> Paths -> Sub-column Slots
    private final Map<Slot, Map<List<String>, SlotReference>> subColumnSlotRefMap
            = Maps.newHashMap();

    // Map from rewritten slot to original expr
    private final Map<Slot, Expression> subColumnOriginalExprMap = Maps.newHashMap();

    // Map from original expr to rewritten slot
    private final Map<Expression, Slot> originalExprToRewrittenSubColumn = Maps.newHashMap();

    // Map slot to its relation, currently used in SlotReference to find its original
    // Relation for example LogicalOlapScan
    private final Map<Slot, Relation> slotToRelation = Maps.newHashMap();

    public StatementContext() {
        this.connectContext = ConnectContext.get();
    }

    public StatementContext(ConnectContext connectContext, OriginStatement originStatement) {
        this.connectContext = connectContext;
        this.originStatement = originStatement;
    }

    public void setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setOriginStatement(OriginStatement originStatement) {
        this.originStatement = originStatement;
    }

    public OriginStatement getOriginStatement() {
        return originStatement;
    }

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    public void setMaxNAryInnerJoin(int maxNAryInnerJoin) {
        if (maxNAryInnerJoin > this.maxNAryInnerJoin) {
            this.maxNAryInnerJoin = maxNAryInnerJoin;
        }
    }

    public int getMaxNAryInnerJoin() {
        return maxNAryInnerJoin;
    }

    public void setMaxContinuousJoin(int joinCount) {
        if (joinCount > this.joinCount) {
            this.joinCount = joinCount;
        }
    }

    public int getMaxContinuousJoin() {
        return joinCount;
    }

    public Set<SlotReference> getAllPathsSlots() {
        Set<SlotReference> allSlotReferences = Sets.newHashSet();
        for (Map<List<String>, SlotReference> slotReferenceMap : subColumnSlotRefMap.values()) {
            allSlotReferences.addAll(slotReferenceMap.values());
        }
        return allSlotReferences;
    }

    public Expression getOriginalExpr(SlotReference rewriteSlot) {
        return subColumnOriginalExprMap.getOrDefault(rewriteSlot, null);
    }

    public Slot getRewrittenSlotRefByOriginalExpr(Expression originalExpr) {
        return originalExprToRewrittenSubColumn.getOrDefault(originalExpr, null);
    }

    /**
     * Add a slot ref attached with paths in context to avoid duplicated slot
     */
    public void addPathSlotRef(Slot root, List<String> paths, SlotReference slotRef, Expression originalExpr) {
        subColumnSlotRefMap.computeIfAbsent(root, k -> Maps.newTreeMap(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> lst1, List<String> lst2) {
                Iterator<String> it1 = lst1.iterator();
                Iterator<String> it2 = lst2.iterator();
                while (it1.hasNext() && it2.hasNext()) {
                    int result = it1.next().compareTo(it2.next());
                    if (result != 0) {
                        return result;
                    }
                }
                return Integer.compare(lst1.size(), lst2.size());
            }
        }));
        subColumnSlotRefMap.get(root).put(paths, slotRef);
        subColumnOriginalExprMap.put(slotRef, originalExpr);
        originalExprToRewrittenSubColumn.put(originalExpr, slotRef);
    }

    public SlotReference getPathSlot(Slot root, List<String> paths) {
        Map<List<String>, SlotReference> pathsSlotsMap = subColumnSlotRefMap.getOrDefault(root, null);
        if (pathsSlotsMap == null) {
            return null;
        }
        return pathsSlotsMap.getOrDefault(paths, null);
    }

    public void addSlotToRelation(Slot slot, Relation relation) {
        slotToRelation.put(slot, relation);
    }

    public Relation getRelationBySlot(Slot slot) {
        return slotToRelation.getOrDefault(slot, null);
    }

    public boolean isDpHyp() {
        return isDpHyp;
    }

    public void setDpHyp(boolean dpHyp) {
        isDpHyp = dpHyp;
    }

    public ExprId getNextExprId() {
        return exprIdGenerator.getNextId();
    }

    public CTEId getNextCTEId() {
        return cteIdGenerator.getNextId();
    }

    public ObjectId getNextObjectId() {
        return objectIdGenerator.getNextId();
    }

    public RelationId getNextRelationId() {
        return relationIdGenerator.getNextId();
    }

    public void setParsedStatement(StatementBase parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    /** getOrRegisterCache */
    public synchronized <T> T getOrRegisterCache(String key, Supplier<T> cacheSupplier) {
        Supplier<T> supplier = (Supplier<T>) contextCacheMap.get(key);
        if (supplier == null) {
            contextCacheMap.put(key, (Supplier<Object>) Suppliers.memoize(cacheSupplier));
            supplier = cacheSupplier;
        }
        return supplier.get();
    }

    /**
     * Some value of the cacheKey may change, invalid cache when value change
     */
    public synchronized void invalidCache(String cacheKey) {
        contextCacheMap.remove(cacheKey);
    }

    public ColumnAliasGenerator getColumnAliasGenerator() {
        return columnAliasGenerator == null
            ? columnAliasGenerator = new ColumnAliasGenerator()
            : columnAliasGenerator;
    }

    public String generateColumnName() {
        return getColumnAliasGenerator().getNextAlias();
    }

    public StatementBase getParsedStatement() {
        return parsedStatement;
    }

    public Map<CTEId, Set<LogicalCTEConsumer>> getCteIdToConsumers() {
        return cteIdToConsumers;
    }

    public Map<CTEId, Set<NamedExpression>> getCteIdToProjects() {
        return cteIdToProjects;
    }

    public Map<RelationId, Set<Expression>> getConsumerIdToFilters() {
        return consumerIdToFilters;
    }

    public Map<CTEId, Set<RelationId>> getCteIdToConsumerUnderProjects() {
        return cteIdToConsumerUnderProjects;
    }

    public Map<CTEId, List<Pair<Map<Slot, Slot>, Group>>> getCteIdToConsumerGroup() {
        return cteIdToConsumerGroup;
    }

    public Map<CTEId, LogicalPlan> getRewrittenCteProducer() {
        return rewrittenCteProducer;
    }

    public Map<CTEId, LogicalPlan> getRewrittenCteConsumer() {
        return rewrittenCteConsumer;
    }

    public void addViewDdlSql(String ddlSql) {
        this.viewDdlSqlSet.add(ddlSql);
    }

    public List<String> getViewDdlSqls() {
        return ImmutableList.copyOf(viewDdlSqlSet);
    }

    public void addHint(Hint hint) {
        this.hints.add(hint);
    }

    public List<Hint> getHints() {
        return ImmutableList.copyOf(hints);
    }

    public List<Expression> getJoinFilters() {
        return joinFilters;
    }

    public void addJoinFilters(Collection<Expression> newJoinFilters) {
        this.joinFilters.addAll(newJoinFilters);
    }

    public boolean isHasUnknownColStats() {
        return hasUnknownColStats;
    }

    public void setHasUnknownColStats(boolean hasUnknownColStats) {
        this.hasUnknownColStats = hasUnknownColStats;
    }
}
