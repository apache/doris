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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.ExpressionPosition;
import org.apache.doris.nereids.rules.exploration.mv.mapping.Mapping.MappedRelation;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * For outer join we should check the outer join compatibility between query and view
 */
public class LogicalCompatibilityContext {
    private final BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping;
    private final BiMap<Integer, Integer> queryToViewNodeIDMapping;
    private final ObjectId planNodeId;
    private final Supplier<BiMap<Expression, Expression>> queryToViewJoinEdgeExpressionMappingSupplier;
    private final Supplier<BiMap<Expression, Expression>> queryToViewNodeExpressionMappingSupplier;
    private final Supplier<BiMap<Expression, Expression>> queryToViewFilterEdgeExpressionMappingSupplier;
    @Deprecated
    private BiMap<Expression, Expression> queryToViewAllExpressionMapping;

    /**
     * LogicalCompatibilityContext
     */
    private LogicalCompatibilityContext(BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping,
            Map<SlotReference, SlotReference> viewToQuerySlotMapping, StructInfo queryStructInfo,
            StructInfo viewStructInfo) {

        this.queryToViewJoinEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> generateExpressionMapping(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.JOIN_EDGE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.JOIN_EDGE)));

        this.queryToViewNodeExpressionMappingSupplier =
                Suppliers.memoize(() -> generateExpressionMapping(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.NODE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.NODE)));

        this.queryToViewFilterEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> generateExpressionMapping(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE)));

        this.queryToViewNodeMapping = queryToViewNodeMapping;
        this.queryToViewNodeIDMapping = HashBiMap.create();
        queryToViewNodeMapping.forEach((k, v) -> queryToViewNodeIDMapping.put(k.getIndex(), v.getIndex()));

        this.planNodeId = queryStructInfo.getTopPlan().getGroupExpression()
                .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1));
    }

    public BiMap<StructInfoNode, StructInfoNode> getQueryToViewNodeMapping() {
        return queryToViewNodeMapping;
    }

    public BiMap<Integer, Integer> getQueryToViewNodeIDMapping() {
        return queryToViewNodeIDMapping;
    }

    public Expression getViewJoinExprFromQuery(Expression queryJoinExpr) {
        return queryToViewJoinEdgeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    public Expression getViewFilterExprFromQuery(Expression queryJoinExpr) {
        return queryToViewFilterEdgeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    public Expression getViewNodeExprFromQuery(Expression queryJoinExpr) {
        return queryToViewNodeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    /**
     * Generate logical compatibility context,
     * this make expression mapping between query and view by relation and the slot in relation mapping
     */
    public static LogicalCompatibilityContext from(RelationMapping relationMapping,
            SlotMapping viewToQuerySlotMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo) {
        // init node mapping
        BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping = HashBiMap.create();
        Map<RelationId, StructInfoNode> queryRelationIdStructInfoNodeMap
                = queryStructInfo.getRelationIdStructInfoNodeMap();
        Map<RelationId, StructInfoNode> viewRelationIdStructInfoNodeMap
                = viewStructInfo.getRelationIdStructInfoNodeMap();
        for (Map.Entry<MappedRelation, MappedRelation> relationMappingEntry :
                relationMapping.getMappedRelationMap().entrySet()) {
            StructInfoNode queryStructInfoNode = queryRelationIdStructInfoNodeMap.get(
                    relationMappingEntry.getKey().getRelationId());
            StructInfoNode viewStructInfoNode = viewRelationIdStructInfoNodeMap.get(
                    relationMappingEntry.getValue().getRelationId());
            if (queryStructInfoNode != null && viewStructInfoNode != null) {
                queryToViewNodeMapping.put(queryStructInfoNode, viewStructInfoNode);
            }
        }
        return new LogicalCompatibilityContext(queryToViewNodeMapping,
                viewToQuerySlotMapping.toSlotReferenceMap(),
                queryStructInfo,
                viewStructInfo);
    }

    private static BiMap<Expression, Expression> generateExpressionMapping(
            Map<SlotReference, SlotReference> viewToQuerySlotMapping,
            Map<Expression, Expression> queryShuttledExprToExprMap,
            Map<Expression, Expression> viewShuttledExprToExprMap) {
        final Map<Expression, Expression> viewEdgeToConjunctsMapQueryBased = new HashMap<>();
        BiMap<Expression, Expression> queryToViewEdgeMapping = HashBiMap.create();
        if (queryShuttledExprToExprMap == null || viewShuttledExprToExprMap == null
                || queryShuttledExprToExprMap.isEmpty() || viewShuttledExprToExprMap.isEmpty()) {
            return queryToViewEdgeMapping;
        }
        viewShuttledExprToExprMap.forEach((shuttledExpr, expr) -> {
            viewEdgeToConjunctsMapQueryBased.put(
                    orderSlotAsc(ExpressionUtils.replace(shuttledExpr, viewToQuerySlotMapping)), expr);
        });
        queryShuttledExprToExprMap.forEach((exprSet, edge) -> {
            Expression viewExpr = viewEdgeToConjunctsMapQueryBased.get(orderSlotAsc(exprSet));
            if (viewExpr != null) {
                queryToViewEdgeMapping.put(edge, viewExpr);
            }
        });
        return queryToViewEdgeMapping;
    }

    private static Expression orderSlotAsc(Expression expression) {
        return expression.accept(ExpressionSlotOrder.INSTANCE, null);
    }

    private static final class ExpressionSlotOrder extends DefaultExpressionRewriter<Void> {
        public static final ExpressionSlotOrder INSTANCE = new ExpressionSlotOrder();

        @Override
        public Expression visitEqualTo(EqualTo equalTo, Void context) {
            if (!(equalTo.getArgument(0) instanceof NamedExpression)
                    || !(equalTo.getArgument(1) instanceof NamedExpression)) {
                return equalTo;
            }
            NamedExpression left = (NamedExpression) equalTo.getArgument(0);
            NamedExpression right = (NamedExpression) equalTo.getArgument(1);
            if (right.getExprId().asInt() < left.getExprId().asInt()) {
                return new EqualTo(right, left);
            } else {
                return equalTo;
            }
        }
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCompatibilityContext",
                "queryToViewNodeMapping", queryToViewNodeMapping.toString(),
                "queryToViewJoinEdgeExpressionMapping",
                queryToViewJoinEdgeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewJoinEdgeExpressionMappingSupplier.get().toString(),
                "queryToViewNodeExpressionMapping",
                queryToViewNodeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewNodeExpressionMappingSupplier.get().toString(),
                "queryToViewFilterEdgeExpressionMapping",
                queryToViewFilterEdgeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewFilterEdgeExpressionMappingSupplier.get().toString());
    }
}
