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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** Data-only description used to bind a Paimon row-change sink against the current target schema. */
public abstract class PaimonRowChangeSpec {
    public abstract DMLCommandType getDmlCommandType();

    public abstract List<? extends Expression> getExpressions();

    /** UPDATE description. */
    public static final class Update extends PaimonRowChangeSpec {
        private final String tableAlias;
        private final List<EqualTo> assignments;

        public Update(String tableAlias, List<EqualTo> assignments) {
            this.tableAlias = tableAlias;
            this.assignments = Utils.copyRequiredList(assignments);
        }

        public String getTableAlias() {
            return tableAlias;
        }

        public List<EqualTo> getAssignments() {
            return assignments;
        }

        @Override
        public DMLCommandType getDmlCommandType() {
            return DMLCommandType.UPDATE;
        }

        @Override
        public List<? extends Expression> getExpressions() {
            return assignments;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Update)) {
                return false;
            }
            Update update = (Update) other;
            return Objects.equals(tableAlias, update.tableAlias)
                    && Objects.equals(assignments, update.assignments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableAlias, assignments);
        }
    }

    /** DELETE description. */
    public static final class Delete extends PaimonRowChangeSpec {
        private final String tableAlias;

        public Delete(String tableAlias) {
            this.tableAlias = tableAlias;
        }

        public String getTableAlias() {
            return tableAlias;
        }

        @Override
        public DMLCommandType getDmlCommandType() {
            return DMLCommandType.DELETE;
        }

        @Override
        public List<? extends Expression> getExpressions() {
            return ImmutableList.of();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Delete
                    && Objects.equals(tableAlias, ((Delete) other).tableAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableAlias);
        }
    }

    /** MERGE description. */
    public static final class Merge extends PaimonRowChangeSpec {
        private final List<String> targetNameInPlan;
        private final List<MergeMatchedClause> matchedClauses;
        private final List<MergeNotMatchedClause> notMatchedClauses;

        public Merge(List<String> targetNameInPlan,
                List<MergeMatchedClause> matchedClauses,
                List<MergeNotMatchedClause> notMatchedClauses) {
            this.targetNameInPlan = Utils.copyRequiredList(targetNameInPlan);
            this.matchedClauses = Utils.copyRequiredList(matchedClauses);
            this.notMatchedClauses = Utils.copyRequiredList(notMatchedClauses);
        }

        public List<String> getTargetNameInPlan() {
            return targetNameInPlan;
        }

        public List<MergeMatchedClause> getMatchedClauses() {
            return matchedClauses;
        }

        public List<MergeNotMatchedClause> getNotMatchedClauses() {
            return notMatchedClauses;
        }

        @Override
        public DMLCommandType getDmlCommandType() {
            return DMLCommandType.MERGE;
        }

        @Override
        public List<? extends Expression> getExpressions() {
            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            for (MergeMatchedClause clause : matchedClauses) {
                clause.getCasePredicate().ifPresent(expressions::add);
                expressions.addAll(clause.getAssignments());
            }
            for (MergeNotMatchedClause clause : notMatchedClauses) {
                clause.getCasePredicate().ifPresent(expressions::add);
                expressions.addAll(clause.getRow());
            }
            return expressions.build();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Merge)) {
                return false;
            }
            Merge merge = (Merge) other;
            return Objects.equals(targetNameInPlan, merge.targetNameInPlan)
                    && Objects.equals(matchedClauses, merge.matchedClauses)
                    && Objects.equals(notMatchedClauses, merge.notMatchedClauses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetNameInPlan, matchedClauses, notMatchedClauses);
        }
    }
}
