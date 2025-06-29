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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;

/**
 * context info used in LogicalPlan deep copy
 */
public class DeepCopierContext {
    /**
     * the original SlotReference to new SlotReference map
     */
    public final Map<ExprId, ExprId> exprIdReplaceMap = Maps.newHashMap();

    /**
     * because LogicalApply keep original plan in itself and its right child in the meantime
     * so, we must use exact same output (same ExprIds) relations between the two plan tree
     * to ensure they keep same after deep copy
     */
    private final Map<RelationId, LogicalRelation> relationReplaceMap = Maps.newHashMap();

    private final Map<Optional<String>, Optional<String>> oldQbNameToNewQbName = Maps.newHashMap();

    private StatementContext statementContext;

    /**
     * DeepCopierContext
     */
    public DeepCopierContext() {
        if (ConnectContext.get() != null) {
            statementContext = ConnectContext.get().getStatementContext();
        }
        if (statementContext == null) {
            statementContext = new StatementContext();
        }
    }

    public void putRelation(RelationId relationId, LogicalRelation newRelation) {
        relationReplaceMap.put(relationId, newRelation);
    }

    public Map<RelationId, LogicalRelation> getRelationReplaceMap() {
        return relationReplaceMap;
    }

    /**
     * copyQbName
     */
    public Optional<String> copyQbName(Optional<String> qbName) {
        Optional<String> newQbName = oldQbNameToNewQbName.get(qbName);
        if (newQbName == null) {
            newQbName = Optional.of(statementContext.getNextQbName());
            oldQbNameToNewQbName.put(qbName, newQbName);
            statementContext.addQbNameMapping(qbName, newQbName);
        }
        return newQbName;
    }
}
