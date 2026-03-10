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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Translation layer predicate that generates TExprNodeType::SEARCH_EXPR
 * for BE VSearchExpr processing. This is only used during FE->BE translation.
 */
public class SearchPredicate extends Predicate {
    private final String dslString;
    private final QsPlan qsPlan;
    private final List<Index> fieldIndexes;

    public SearchPredicate(String dslString, QsPlan qsPlan, List<Expr> children, boolean nullable) {
        this(dslString, qsPlan, children, Collections.emptyList(), nullable);
    }

    public SearchPredicate(String dslString, QsPlan qsPlan, List<Expr> children,
            List<Index> fieldIndexes, boolean nullable) {
        super();
        this.dslString = dslString;
        this.qsPlan = qsPlan;
        this.fieldIndexes = fieldIndexes != null ? fieldIndexes : Collections.emptyList();
        this.type = Type.BOOLEAN;

        // Add children (SlotReferences)
        if (children != null) {
            this.children.addAll(children);
        }
        this.nullable = nullable;
    }

    protected SearchPredicate(SearchPredicate other) {
        super(other);
        this.dslString = other.dslString;
        this.qsPlan = other.qsPlan;
        this.fieldIndexes = other.fieldIndexes;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitSearchPredicate(this, context);
    }

    @Override
    public Expr clone() {
        return new SearchPredicate(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        SearchPredicate that = (SearchPredicate) obj;
        return dslString.equals(that.dslString);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), dslString);
    }

    boolean isExplainVerboseContext() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return false;
        }
        StmtExecutor executor = ctx.getExecutor();
        if (executor == null || executor.getParsedStmt() == null
                || executor.getParsedStmt().getExplainOptions() == null) {
            return false;
        }
        return executor.getParsedStmt().getExplainOptions().isVerbose();
    }

    List<String> buildFieldBindingExplainLines() {
        List<String> lines = new ArrayList<>();
        if (qsPlan == null || qsPlan.getFieldBindings() == null || qsPlan.getFieldBindings().isEmpty()) {
            return lines;
        }
        IntStream.range(0, qsPlan.getFieldBindings().size()).forEach(index -> {
            SearchDslParser.QsFieldBinding binding = qsPlan.getFieldBindings().get(index);
            String slotDesc = "<unbound>";
            if (index < children.size() && children.get(index) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) children.get(index);
                slotDesc = slotRef.getSlotId() != null
                        ? "slot=" + slotRef.getSlotId().asInt()
                        : slotRef.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
            } else if (index < children.size()) {
                slotDesc = children.get(index).accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
            }
            lines.add(binding.getFieldName() + " -> " + slotDesc);
        });
        return lines;
    }

    public String getDslString() {
        return dslString;
    }

    public QsPlan getQsPlan() {
        return qsPlan;
    }

    public List<Index> getFieldIndexes() {
        return fieldIndexes;
    }
}
