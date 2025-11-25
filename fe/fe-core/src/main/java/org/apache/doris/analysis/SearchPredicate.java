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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSearchClause;
import org.apache.doris.thrift.TSearchFieldBinding;
import org.apache.doris.thrift.TSearchParam;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
    protected String toSqlImpl() {
        return buildSqlForExplain();
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql,
            org.apache.doris.catalog.TableIf.TableType tableType,
            org.apache.doris.catalog.TableIf table) {
        return buildSqlForExplain();
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
                LOG.info("SearchPredicate.toThrift: SlotRef details - column={}",
                        slotRef.getColumnName());
                if (slotRef.isAnalyzed() && slotRef.getDesc() != null) {
                    LOG.info("SearchPredicate.toThrift: SlotRef analyzed - slotId={}",
                            slotRef.getSlotId());
                }
            }
        }
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

    private TSearchParam buildThriftParam() {
        TSearchParam param = new TSearchParam();
        param.setOriginalDsl(dslString);
        param.setRoot(convertQsNodeToThrift(qsPlan.root));

        List<TSearchFieldBinding> bindings = new ArrayList<>();
        for (int i = 0; i < qsPlan.fieldBindings.size(); i++) {
            SearchDslParser.QsFieldBinding binding = qsPlan.fieldBindings.get(i);
            TSearchFieldBinding thriftBinding = new TSearchFieldBinding();

            String fieldPath = binding.fieldName;
            thriftBinding.setFieldName(fieldPath);

            // Check if this is a variant subcolumn (contains dot)
            if (fieldPath.contains(".")) {
                // Parse variant subcolumn path
                int firstDotPos = fieldPath.indexOf('.');
                String parentField = fieldPath.substring(0, firstDotPos);
                String subcolumnPath = fieldPath.substring(firstDotPos + 1);

                thriftBinding.setIsVariantSubcolumn(true);
                thriftBinding.setParentFieldName(parentField);
                thriftBinding.setSubcolumnPath(subcolumnPath);

                LOG.info("buildThriftParam: variant subcolumn field='{}', parent='{}', subcolumn='{}'",
                        fieldPath, parentField, subcolumnPath);
            } else {
                thriftBinding.setIsVariantSubcolumn(false);
            }

            // Set slot index - this is the index in the children array, not the slotId
            thriftBinding.setSlotIndex(i);

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

    private String buildSqlForExplain() {
        if (!isExplainVerboseContext()) {
            return "search('" + dslString + "')";
        }

        StringBuilder sb = new StringBuilder("search('" + dslString + "')");

        List<String> astLines = buildDslAstExplainLines();
        if (!astLines.isEmpty()) {
            sb.append("\n|      dsl_ast:");
            for (String line : astLines) {
                sb.append("\n|        ").append(line);
            }
        }

        List<String> bindings = buildFieldBindingExplainLines();
        if (!bindings.isEmpty()) {
            sb.append("\n|      field_bindings:");
            for (String binding : bindings) {
                sb.append("\n|        ").append(binding);
            }
        }

        return sb.toString();
    }

    private boolean isExplainVerboseContext() {
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

    private List<String> buildDslAstExplainLines() {
        List<String> lines = new ArrayList<>();
        if (qsPlan == null || qsPlan.root == null) {
            return lines;
        }
        TSearchClause rootClause = convertQsNodeToThrift(qsPlan.root);
        appendClauseExplain(rootClause, lines, 0);
        return lines;
    }

    private void appendClauseExplain(TSearchClause clause, List<String> lines, int depth) {
        StringBuilder line = new StringBuilder();
        line.append(indent(depth)).append("- clause_type=").append(clause.getClauseType());
        if (clause.isSetFieldName()) {
            line.append(", field=").append('\"').append(escapeText(clause.getFieldName())).append('\"');
        }
        if (clause.isSetValue()) {
            line.append(", value=").append('\"').append(escapeText(clause.getValue())).append('\"');
        }
        lines.add(line.toString());

        if (clause.isSetChildren() && clause.getChildren() != null && !clause.getChildren().isEmpty()) {
            for (TSearchClause child : clause.getChildren()) {
                appendClauseExplain(child, lines, depth + 1);
            }
        }
    }

    private List<String> buildFieldBindingExplainLines() {
        List<String> lines = new ArrayList<>();
        if (qsPlan == null || qsPlan.fieldBindings == null || qsPlan.fieldBindings.isEmpty()) {
            return lines;
        }
        IntStream.range(0, qsPlan.fieldBindings.size()).forEach(index -> {
            SearchDslParser.QsFieldBinding binding = qsPlan.fieldBindings.get(index);
            String slotDesc = "<unbound>";
            if (index < children.size() && children.get(index) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) children.get(index);
                slotDesc = slotRef.getSlotId() != null
                        ? "slot=" + slotRef.getSlotId().asInt()
                        : slotRef.toSqlWithoutTbl();
            } else if (index < children.size()) {
                slotDesc = children.get(index).toSqlWithoutTbl();
            }
            lines.add(binding.fieldName + " -> " + slotDesc);
        });
        return lines;
    }

    private String indent(int level) {
        if (level <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(level * 2);
        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }

    private String escapeText(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
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

    // Getters
    public String getDslString() {
        return dslString;
    }

    public QsPlan getQsPlan() {
        return qsPlan;
    }
}
