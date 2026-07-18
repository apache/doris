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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builds the stable IVM maintenance-layout signature.
 *
 * <p>The signature is determined only by the normalized MV plan. It is unrelated to delta rewrite: once the
 * normalized plan is fixed, the row-id computation is fixed, so the signature is fixed as well. The signature only
 * tracks the row-id generation path because row-id is used to locate existing MV rows when incremental refresh
 * retracts or merges data. Delta rewrite may evolve independently; its generated row-id must adapt to the normalized
 * plan instead of changing the normalized-plan signature.
 */
public class IvmPlanSignatureGenerator {
    public static final int CURRENT_VERSION = 1;
    @VisibleForTesting
    public static final String DEBUG_POINT_SIGNATURE_SALT =
            "IvmPlanSignatureGenerator.generate.signature_salt";
    private static final String HEADER = "IVM_LAYOUT_SIGNATURE_V" + CURRENT_VERSION;

    private final CanonicalExpressionVisitor canonicalExpressionVisitor = new CanonicalExpressionVisitor();
    private final CanonicalPlanVisitor canonicalPlanVisitor = new CanonicalPlanVisitor();

    public IvmPlanSignatureGenerator() {
    }

    public IvmPlanSignature generate(Plan normalizedPlan) {
        CanonicalNode root = CanonicalNode.node("ROOT")
                .field("plan", canonicalPlan(normalizedPlan));
        String canonical = HEADER + "\n" + root.encoded();
        // Test hook for simulating analyzed-plan layout drift without rebuilding a separate plan.
        String debugSalt = DebugPointUtil.getDebugParamOrDefault(DEBUG_POINT_SIGNATURE_SALT, "");
        if (!debugSalt.isEmpty()) {
            canonical = canonical + "\nDEBUG_SIGNATURE_SALT=" + debugSalt;
        }
        return new IvmPlanSignature(canonical, sha256(canonical));
    }

    private CanonicalNode canonicalPlan(Plan plan) {
        return plan.accept(canonicalPlanVisitor, null);
    }

    private CanonicalNode canonicalProject(LogicalProject<?> project) {
        if (isPassthroughProject(project)) {
            return canonicalPlan(project.child());
        }
        return CanonicalNode.node("PROJECT")
                .field("hiddenOutputs", canonicalHiddenNamedExpressions(project.getProjects()))
                .field("child", canonicalPlan(project.child()));
    }

    private boolean isPassthroughProject(LogicalProject<?> project) {
        return project.getProjects().stream().allMatch(this::isSignatureNeutralProjection);
    }

    private boolean isSignatureNeutralProjection(NamedExpression expression) {
        return isPassthroughExpression(expression) || isIgnorableSinkHiddenProjection(expression);
    }

    private boolean isPassthroughExpression(NamedExpression expression) {
        if (expression instanceof SlotReference) {
            return true;
        }
        if (expression instanceof Alias && ((Alias) expression).child() instanceof SlotReference) {
            return expression.getName().equals(((SlotReference) ((Alias) expression).child()).getName());
        }
        return false;
    }

    private boolean isIgnorableSinkHiddenProjection(NamedExpression expression) {
        return IvmUtil.isCommonHiddenSlot(expression.getName());
    }

    private CanonicalNode canonicalAggregate(LogicalAggregate<?> agg) {
        return CanonicalNode.node("AGG")
                .field("hiddenOutputs", canonicalHiddenNamedExpressions(agg.getOutputExpressions()))
                .field("child", canonicalPlan(agg.child()));
    }

    private CanonicalNode canonicalScan(LogicalOlapScan scan) {
        OlapTable table = scan.getTable();
        return CanonicalNode.node("SCAN")
                .field("table", tableIdentity(table, scan.getQualifier()));
    }

    private CanonicalNode canonicalJoin(LogicalJoin<?, ?> join) {
        return CanonicalNode.node("JOIN")
                .field("joinType", join.getJoinType().name())
                .field("left", canonicalPlan(join.left()))
                .field("right", canonicalPlan(join.right()));
    }

    private CanonicalNode canonicalUnion(LogicalUnion union) {
        CanonicalList arms = CanonicalList.list();
        for (int i = 0; i < union.children().size(); i++) {
            arms.add(CanonicalNode.node("UNION_ARM")
                    .field("index", i)
                    .field("plan", canonicalPlan(union.child(i))));
        }
        return CanonicalNode.node("UNION")
                .field("hiddenOutputs", canonicalHiddenNamedExpressions(union.getOutputs()))
                .field("arms", arms);
    }

    private CanonicalList canonicalHiddenNamedExpressions(List<? extends NamedExpression> expressions) {
        List<CanonicalNode> result = new ArrayList<>();
        for (NamedExpression expression : expressions) {
            if (!IvmUtil.isIvmHiddenColumn(expression.getName())) {
                continue;
            }
            result.add(canonicalNamedExpression(expression));
        }
        return sortedCanonicalList(result);
    }

    private CanonicalList sortedCanonicalList(List<CanonicalNode> nodes) {
        nodes.sort(Comparator.comparing(CanonicalNode::encoded));
        CanonicalList result = CanonicalList.list();
        for (CanonicalNode node : nodes) {
            result.add(node);
        }
        return result;
    }

    private CanonicalNode canonicalNamedExpression(NamedExpression expression) {
        return CanonicalNode.node("NAMED_EXPR")
                .field("name", expression.getName())
                .field("expr", canonicalExpressionNode(expression));
    }

    private CanonicalList canonicalExpressions(List<? extends Expression> expressions) {
        CanonicalList result = CanonicalList.list();
        for (Expression expression : expressions) {
            result.add(canonicalExpressionNode(expression));
        }
        return result;
    }

    @VisibleForTesting
    String canonicalExpression(Expression expression) {
        return canonicalExpressionNode(expression).encoded();
    }

    private CanonicalNode canonicalExpressionNode(Expression expression) {
        return expression.accept(canonicalExpressionVisitor, null);
    }

    private CanonicalNode canonicalGenericExpression(String nodeName, Expression expression) {
        // Keep a custom recursive child signature so SlotReference can use stable table/column identity.
        // Plain Expression#toSql() would print slot names only, so different tables with the same column
        // name could collapse to the same layout string. The sql field still records expression-level
        // semantics, such as cast target types or function syntax, without replacing slot canonicalization.
        return CanonicalNode.node(nodeName)
                .field("class", expression.getClass().getSimpleName())
                .field("sql", expression.toSql())
                .field("children", canonicalExpressions(expression.children()));
    }

    private CanonicalNode canonicalSlot(Slot slot) {
        if (slot instanceof SlotReference) {
            SlotReference slotReference = (SlotReference) slot;
            CanonicalNode node = CanonicalNode.node("SLOT");
            if (slotReference.getOriginalTable().isPresent() && slotReference.getOriginalColumn().isPresent()) {
                TableIf table = slotReference.getOriginalTable().get();
                String columnName = slotReference.getOriginalColumn().get().getName();
                return node.field("identity", tableIdentity(table, slotReference.getQualifier()) + "." + columnName)
                        .field("subPath", canonicalSubPath(slotReference));
            }
            return node.field("qualifier", canonicalQualifier(slotReference.getQualifier()))
                    .field("name", slot.getName())
                    .field("subPath", canonicalSubPath(slotReference));
        }
        return CanonicalNode.node("SLOT")
                .field("name", slot.getName());
    }

    private String tableIdentity(TableIf table, List<String> fallbackQualifier) {
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo != null) {
            return tableNameInfo.toString();
        }
        if (!fallbackQualifier.isEmpty()) {
            return String.join(".", fallbackQualifier) + "." + table.getName();
        }
        if (table instanceof Table && StringUtils.isNotEmpty(((Table) table).getQualifiedDbName())) {
            return ((Table) table).getQualifiedDbName() + "." + table.getName();
        }
        return table.getName();
    }

    private CanonicalList canonicalQualifier(List<String> qualifier) {
        CanonicalList result = CanonicalList.list();
        for (String item : qualifier) {
            result.add(item);
        }
        return result;
    }

    private CanonicalList canonicalSubPath(SlotReference slotReference) {
        CanonicalList result = CanonicalList.list();
        for (String item : slotReference.getSubPath()) {
            result.add(item);
        }
        return result;
    }

    private static String sha256(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }

    private static CanonicalValue canonicalValue(Object value) {
        if (value instanceof CanonicalValue) {
            return (CanonicalValue) value;
        }
        return new CanonicalScalar(String.valueOf(value));
    }

    private interface CanonicalValue {
        void encodeTo(StringBuilder builder);

        default String encoded() {
            StringBuilder builder = new StringBuilder();
            encodeTo(builder);
            return builder.toString();
        }
    }

    private static class CanonicalScalar implements CanonicalValue {
        private final String value;

        private CanonicalScalar(String value) {
            this.value = value;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            appendEscaped(builder, value);
        }
    }

    private static class CanonicalList implements CanonicalValue {
        private final List<CanonicalValue> values = new ArrayList<>();

        private static CanonicalList list() {
            return new CanonicalList();
        }

        private CanonicalList add(Object value) {
            values.add(canonicalValue(value));
            return this;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            builder.append('[');
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                values.get(i).encodeTo(builder);
            }
            builder.append(']');
        }
    }

    private static class CanonicalNode implements CanonicalValue {
        private final String name;
        private final List<CanonicalField> fields = new ArrayList<>();

        private CanonicalNode(String name) {
            this.name = name;
        }

        private static CanonicalNode node(String name) {
            return new CanonicalNode(name);
        }

        private CanonicalNode field(String name, Object value) {
            fields.add(new CanonicalField(name, canonicalValue(value)));
            return this;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            appendEscaped(builder, name);
            builder.append('[');
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                CanonicalField field = fields.get(i);
                appendEscaped(builder, field.name);
                builder.append('=');
                field.value.encodeTo(builder);
            }
            builder.append(']');
        }
    }

    private static class CanonicalField {
        private final String name;
        private final CanonicalValue value;

        private CanonicalField(String name, CanonicalValue value) {
            this.name = name;
            this.value = value;
        }
    }

    private static void appendEscaped(StringBuilder builder, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                case '[':
                case ']':
                case '{':
                case '}':
                case ',':
                case '=':
                case ':':
                    builder.append('\\').append(c);
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    builder.append(c);
                    break;
            }
        }
    }

    private class CanonicalPlanVisitor extends PlanVisitor<CanonicalNode, Void> {
        @Override
        public CanonicalNode visit(Plan plan, Void context) {
            throw new IllegalStateException("Unexpected plan node in IVM layout signature: "
                    + plan.getClass().getSimpleName());
        }

        @Override
        public CanonicalNode visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, Void context) {
            return sink.child().accept(this, context);
        }

        @Override
        public CanonicalNode visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> sink, Void context) {
            return sink.child().accept(this, context);
        }

        @Override
        public CanonicalNode visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
            return filter.child().accept(this, context);
        }

        @Override
        public CanonicalNode visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
            return canonicalProject(project);
        }

        @Override
        public CanonicalNode visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias, Void context) {
            return alias.child().accept(this, context);
        }

        @Override
        public CanonicalNode visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
            return canonicalScan(scan);
        }

        @Override
        public CanonicalNode visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
            return canonicalJoin(join);
        }

        @Override
        public CanonicalNode visitLogicalUnion(LogicalUnion union, Void context) {
            return canonicalUnion(union);
        }

        @Override
        public CanonicalNode visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, Void context) {
            return canonicalAggregate(agg);
        }

        @Override
        public CanonicalNode visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
            return CanonicalNode.node("REPEAT")
                    .field("child", canonicalPlan(repeat.child()));
        }
    }

    private class CanonicalExpressionVisitor extends ExpressionVisitor<CanonicalNode, Void> {
        @Override
        public CanonicalNode visit(Expression expression, Void context) {
            return canonicalGenericExpression("EXPR", expression);
        }

        @Override
        public CanonicalNode visitAlias(Alias alias, Void context) {
            // Alias#toSql() includes the alias name, but arbitrary alias renames must not change the
            // maintenance layout signature. Hidden output names are recorded by canonicalNamedExpression().
            return CanonicalNode.node("ALIAS")
                    .field("child", canonicalExpressionNode(alias.child()));
        }

        @Override
        public CanonicalNode visitSlot(Slot slot, Void context) {
            // SlotReference needs stable table/column identity instead of SQL text, because Expression#toSql()
            // only carries the slot name and may collapse different tables that share the same column name.
            return canonicalSlot(slot);
        }
    }
}
