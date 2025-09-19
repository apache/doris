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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsPlan;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSearchClause;
import org.apache.doris.thrift.TSearchFieldBinding;
import org.apache.doris.thrift.TSearchParam;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Translation layer predicate that generates TExprNodeType::SEARCH_EXPR
 * for BE VSearchExpr processing. This is only used during FE->BE translation.
 */
public class SearchPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(SearchPredicate.class);

    private final String dslString;
    private final QsPlan qsPlan;

    public SearchPredicate(String dslString, QsPlan qsPlan, List<Expr> children) {
        super();
        this.dslString = dslString;
        this.qsPlan = qsPlan;
        this.type = Type.BOOLEAN;

        // Add children (SlotReferences)
        if (children != null) {
            this.children.addAll(children);
        }
    }

    protected SearchPredicate(SearchPredicate other) {
        super(other);
        this.dslString = other.dslString;
        this.qsPlan = other.qsPlan;
    }

    @Override
    public Expr clone() {
        return new SearchPredicate(this);
    }


    @Override
    protected String toSqlImpl() {
        return "search('" + dslString + "')";
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql,
            org.apache.doris.catalog.TableIf.TableType tableType,
            org.apache.doris.catalog.TableIf table) {
        return "search('" + dslString + "')";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SEARCH_EXPR;
        msg.setSearchParam(buildThriftParam());

        LOG.info("SearchPredicate.toThrift: dsl='{}', num_children_in_base={}, children_size={}",
                dslString, msg.num_children, this.children.size());

        // Print QsPlan details
        if (qsPlan != null) {
            LOG.info("SearchPredicate.toThrift: QsPlan fieldBindings.size={}",
                    qsPlan.fieldBindings != null ? qsPlan.fieldBindings.size() : 0);
            if (qsPlan.fieldBindings != null) {
                for (int i = 0; i < qsPlan.fieldBindings.size(); i++) {
                    SearchDslParser.QsFieldBinding binding = qsPlan.fieldBindings.get(i);
                    LOG.info("SearchPredicate.toThrift: binding[{}] fieldName='{}', slotIndex={}",
                            i, binding.fieldName, binding.slotIndex);
                }
            }
        }

        for (int i = 0; i < this.children.size(); i++) {
            Expr child = this.children.get(i);
            LOG.info("SearchPredicate.toThrift: child[{}] = {} (type={})",
                    i, child.getClass().getSimpleName(), child.getType());
            if (child instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) child;
                LOG.info("SearchPredicate.toThrift: SlotRef details - column={}, isAnalyzed={}",
                        slotRef.getColumnName(), slotRef.isAnalyzed());
                if (slotRef.isAnalyzed() && slotRef.getDesc() != null) {
                    LOG.info("SearchPredicate.toThrift: SlotRef analyzed - slotId={}",
                            slotRef.getSlotId());
                }
            }
        }
    }

    private TSearchParam buildThriftParam() {
        TSearchParam param = new TSearchParam();
        param.setOriginalDsl(dslString);
        param.setRoot(convertQsNodeToThrift(qsPlan.root));

        List<TSearchFieldBinding> bindings = new ArrayList<>();
        for (int i = 0; i < qsPlan.fieldBindings.size(); i++) {
            SearchDslParser.QsFieldBinding binding = qsPlan.fieldBindings.get(i);
            TSearchFieldBinding thriftBinding = new TSearchFieldBinding();
            thriftBinding.setFieldName(binding.fieldName);

            if (i < this.children.size() && this.children.get(i) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) this.children.get(i);
                int actualSlotId = slotRef.getSlotId().asInt();
                thriftBinding.setSlotIndex(actualSlotId);
                LOG.info("buildThriftParam: binding field='{}', actual slotId={}", binding.fieldName, actualSlotId);
            } else {
                LOG.warn("buildThriftParam: No corresponding SlotRef for field '{}'", binding.fieldName);
                thriftBinding.setSlotIndex(i); // fallback to position
            }

            bindings.add(thriftBinding);
        }
        param.setFieldBindings(bindings);

        return param;
    }

    private TSearchClause convertQsNodeToThrift(
            org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsNode node) {
        TSearchClause clause = new TSearchClause();

        // Convert clause type
        clause.setClauseType(node.type.name());

        if (node.field != null) {
            clause.setFieldName(node.field);
        }

        if (node.value != null) {
            clause.setValue(node.value);
        }

        if (node.children != null && !node.children.isEmpty()) {
            List<TSearchClause> childClauses = new ArrayList<>();
            for (SearchDslParser.QsNode child : node.children) {
                childClauses.add(convertQsNodeToThrift(child));
            }
            clause.setChildren(childClauses);
        }

        return clause;
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

    // Getters
    public String getDslString() {
        return dslString;
    }

    public QsPlan getQsPlan() {
        return qsPlan;
    }
}
