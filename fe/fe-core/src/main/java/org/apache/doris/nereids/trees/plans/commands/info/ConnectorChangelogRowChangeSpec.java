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

/** Data-only description for a connector whose row-level writes are encoded as changelog rows. */
public abstract class ConnectorChangelogRowChangeSpec {
    public abstract DMLCommandType getDmlCommandType();

    public abstract List<? extends Expression> getExpressions();

    /** UPDATE description. */
    public static final class Update extends ConnectorChangelogRowChangeSpec {
        private final List<String> targetNameInPlan;
        private final List<EqualTo> assignments;

        public Update(List<String> targetNameInPlan, List<EqualTo> assignments) {
            this.targetNameInPlan = Utils.copyRequiredList(targetNameInPlan);
            this.assignments = Utils.copyRequiredList(assignments);
        }

        public List<String> getTargetNameInPlan() {
            return targetNameInPlan;
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
            return other instanceof Update
                    && Objects.equals(targetNameInPlan, ((Update) other).targetNameInPlan)
                    && Objects.equals(assignments, ((Update) other).assignments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetNameInPlan, assignments);
        }
    }

    /** DELETE description. */
    public static final class Delete extends ConnectorChangelogRowChangeSpec {
        private final List<String> targetNameInPlan;
        private final boolean deduplicateTargetRows;

        public Delete(List<String> targetNameInPlan, boolean deduplicateTargetRows) {
            this.targetNameInPlan = Utils.copyRequiredList(targetNameInPlan);
            this.deduplicateTargetRows = deduplicateTargetRows;
        }

        public List<String> getTargetNameInPlan() {
            return targetNameInPlan;
        }

        public boolean shouldDeduplicateTargetRows() {
            return deduplicateTargetRows;
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
                    && Objects.equals(targetNameInPlan, ((Delete) other).targetNameInPlan)
                    && deduplicateTargetRows == ((Delete) other).deduplicateTargetRows;
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetNameInPlan, deduplicateTargetRows);
        }
    }

    /** MERGE description. */
    public static final class Merge extends ConnectorChangelogRowChangeSpec {
        private final List<String> targetNameInPlan;
        private final List<MergeMatchedClause> matchedClauses;
        private final List<MergeNotMatchedClause> notMatchedClauses;

        public Merge(List<String> targetNameInPlan, List<MergeMatchedClause> matchedClauses,
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
            return other instanceof Merge
                    && Objects.equals(targetNameInPlan, ((Merge) other).targetNameInPlan)
                    && Objects.equals(matchedClauses, ((Merge) other).matchedClauses)
                    && Objects.equals(notMatchedClauses, ((Merge) other).notMatchedClauses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetNameInPlan, matchedClauses, notMatchedClauses);
        }
    }
}
