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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;

/**
 * StructInfo
 */
public class StructInfo {
    private final List<CatalogRelation> relations;
    private final Predicates predicates;
    // Used by predicate compensation
    private final EquivalenceClass equivalenceClass;
    private final Plan originalPlan;
    private final HyperGraph hyperGraph;

    private StructInfo(List<CatalogRelation> relations,
            Predicates predicates,
            Plan originalPlan,
            HyperGraph hyperGraph) {
        this.relations = relations;
        this.predicates = predicates;
        this.originalPlan = originalPlan;
        this.hyperGraph = hyperGraph;
        // construct equivalenceClass according to equals predicates
        this.equivalenceClass = new EquivalenceClass();
        SplitPredicate splitPredicate = Predicates.splitPredicates(predicates.composedExpression());
        for (Expression expression : ExpressionUtils.extractConjunction(splitPredicate.getEqualPredicates())) {
            EqualTo equalTo = (EqualTo) expression;
            equivalenceClass.addEquivalenceClass(
                    (SlotReference) equalTo.getArguments().get(0),
                    (SlotReference) equalTo.getArguments().get(1));
        }
    }

    public static StructInfo of(Plan originalPlan) {
        // TODO build graph from original plan and get relations and predicates from graph
        return new StructInfo(null, null, originalPlan, null);
    }

    public static StructInfo of(Group group) {
        // TODO build graph from original plan and get relations and predicates from graph
        return new StructInfo(null, null, group.getLogicalExpression().getPlan(), null);
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public Predicates getPredicates() {
        return predicates;
    }

    public EquivalenceClass getEquivalenceClass() {
        return equivalenceClass;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    public List<? extends Expression> getExpressions() {
        return originalPlan instanceof LogicalProject
                ? ((LogicalProject<Plan>) originalPlan).getProjects() : originalPlan.getOutput();
    }

    /**
     * Judge the source graph logical is whether the same as target
     * For inner join should judge only the join tables,
     * for other join type should also judge the join direction, it's input filter that can not be pulled up etc.
     * */
    public static boolean isGraphLogicalEquals(HyperGraph source, HyperGraph target) {
        return false;
    }
}
