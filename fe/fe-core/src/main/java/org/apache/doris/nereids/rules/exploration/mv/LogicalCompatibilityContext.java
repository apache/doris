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
import org.apache.doris.nereids.rules.exploration.mv.mapping.Mapping.MappedRelation;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;

/**
 * For outer join we should check the outer join compatibility between query and view
 */
public class LogicalCompatibilityContext {
    private final BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping;
    private final BiMap<Expression, Expression> queryToViewEdgeExpressionMapping;
    private final BiMap<Integer, Integer> queryToViewNodeIDMapping;

    public LogicalCompatibilityContext(BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping,
            BiMap<Expression, Expression> queryToViewEdgeExpressionMapping) {
        this.queryToViewNodeMapping = queryToViewNodeMapping;
        this.queryToViewEdgeExpressionMapping = queryToViewEdgeExpressionMapping;
        this.queryToViewNodeIDMapping = HashBiMap.create();
        queryToViewNodeMapping.forEach((k, v) -> queryToViewNodeIDMapping.put(k.getIndex(), v.getIndex()));
    }

    public BiMap<StructInfoNode, StructInfoNode> getQueryToViewNodeMapping() {
        return queryToViewNodeMapping;
    }

    public BiMap<Integer, Integer> getQueryToViewNodeIDMapping() {
        return queryToViewNodeIDMapping;
    }

    public BiMap<Expression, Expression> getQueryToViewEdgeExpressionMapping() {
        return queryToViewEdgeExpressionMapping;
    }

    /**
     * generate logical compatibility context
     */
    public static LogicalCompatibilityContext from(RelationMapping relationMapping,
            SlotMapping slotMapping,
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
        // init expression mapping
        Map<SlotReference, SlotReference> viewToQuerySlotMapping = slotMapping.inverse().toSlotReferenceMap();
        Map<Expression, Expression> queryShuttledExprToExprMap =
                queryStructInfo.getShuttledHashConjunctsToConjunctsMap();
        Map<Expression, Expression> viewShuttledExprToExprMap =
                viewStructInfo.getShuttledHashConjunctsToConjunctsMap();
        final Map<Expression, Expression> viewEdgeToConjunctsMapQueryBased = new HashMap<>();
        viewShuttledExprToExprMap.forEach((shuttledExpr, expr) -> {
            viewEdgeToConjunctsMapQueryBased.put(
                    orderSlotAsc(ExpressionUtils.replace(shuttledExpr, viewToQuerySlotMapping)),
                    expr);
        });
        BiMap<Expression, Expression> queryToViewEdgeMapping = HashBiMap.create();
        queryShuttledExprToExprMap.forEach((exprSet, edge) -> {
            Expression viewExpr = viewEdgeToConjunctsMapQueryBased.get(orderSlotAsc(exprSet));
            if (viewExpr != null) {
                queryToViewEdgeMapping.put(edge, viewExpr);
            }
        });
        return new LogicalCompatibilityContext(queryToViewNodeMapping, queryToViewEdgeMapping);
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
}
