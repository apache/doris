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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers an {@code ALTER TABLE t EXECUTE proc(...) WHERE <cond>} predicate into an engine-neutral
 * {@link ConnectorPredicate} for a {@code DISTRIBUTED} connector procedure (today: iceberg
 * {@code rewrite_data_files}), so the connector can scope the rewrite to the files matching the {@code WHERE}.
 *
 * <p>This is the procedure-side analogue of {@link WriteConstraintExtractor} (the DELETE/MERGE write-time
 * conflict path), but with two essential differences driven by the {@code EXECUTE} grammar and the rewrite
 * semantics:</p>
 *
 * <ol>
 *   <li><b>The WHERE arrives UNBOUND.</b> {@code ExecuteActionCommand} never runs the Nereids analyzer over
 *       its {@code WHERE} (only the table name is analysed), so column references are {@link UnboundSlot}s
 *       (a bare name from the parser), not bound {@link SlotReference}s. {@link WriteConstraintExtractor} /
 *       {@link NereidsToConnectorExpressionConverter} require bound slots and would silently drop every
 *       unbound leaf — yielding an empty predicate and a whole-table rewrite. Here each leaf column name is
 *       resolved directly against the target table's schema ({@link ExternalTable#getColumn}), mirroring the
 *       legacy {@code IcebergNereidsUtils.extractColumnName} which also accepted the unbound parser form.</li>
 *   <li><b>Fail-loud, never widen.</b> For a write-time conflict filter, dropping an unconvertible conjunct
 *       only <i>widens</i> the filter (safe). For a user-authored rewrite {@code WHERE}, dropping a conjunct
 *       <i>widens the set of files rewritten</i> — at the limit, dropping the whole {@code WHERE} rewrites the
 *       entire table. So this converter is strictly all-or-nothing: if any part of the {@code WHERE} cannot be
 *       represented neutrally it throws (restoring the legacy live-rewrite behaviour, which threw), rather than
 *       producing a partial/empty predicate. The connector enforces the symmetric invariant on its side
 *       (a conjunct it cannot push to file pruning is also a hard error, not a silent drop).</li>
 * </ol>
 *
 * <p><b>Node matrix</b> mirrors {@link NereidsToConnectorExpressionConverter} (the legacy iceberg WHERE node
 * set): {@code And}/{@code Or}/{@code Not}, the five comparisons ({@code EQ}/{@code GT}/{@code GE}/{@code LT}/
 * {@code LE}, column-op-literal), {@code In}, {@code IsNull}, {@code Between}. Literals route through
 * {@link ExprToConnectorExpressionConverter} so the neutral type tokens are byte-identical to the scan /
 * conflict paths; the connector then maps them to its own dialect. The {@link ConnectorColumnRef} carries the
 * column's real type (resolved from the table schema) — accurate rather than a placeholder — even though the
 * iceberg connector resolves the column by name and does not read it.</p>
 *
 * <p>Engine-neutral by construction (no {@code instanceof Iceberg}, no iceberg imports): it speaks only the
 * neutral {@code connector.api.pushdown} vocabulary plus generic Nereids nodes.</p>
 */
public final class UnboundExpressionToConnectorPredicateConverter {

    private UnboundExpressionToConnectorPredicateConverter() {
    }

    /**
     * Lowers the {@code WHERE} predicate to a {@link ConnectorPredicate} over {@code table}'s columns.
     *
     * @throws AnalysisException if any part of the {@code WHERE} cannot be represented neutrally, or references
     *         a column not in the table (fail-loud — never returns a partial predicate that would widen the
     *         rewrite scope).
     */
    public static ConnectorPredicate convert(Expression where, ExternalTable table) throws AnalysisException {
        ConnectorExpression expr = convertNode(where, table);
        if (expr == null) {
            throw new AnalysisException("WHERE condition is not supported for this procedure: " + where.toSql());
        }
        return new ConnectorPredicate(expr);
    }

    // Returns null when the node cannot be represented; every parent treats a null child as "the whole node is
    // unrepresentable" (all-or-nothing), so a single unconvertible leaf fails the entire WHERE at convert().
    private static ConnectorExpression convertNode(Expression expr, ExternalTable table) throws AnalysisException {
        if (expr == null) {
            return null;
        }
        if (expr instanceof And) {
            return convertAnd((And) expr, table);
        } else if (expr instanceof Or) {
            return convertOr((Or) expr, table);
        } else if (expr instanceof Not) {
            ConnectorExpression child = convertNode(((Not) expr).child(), table);
            return child == null ? null : new ConnectorNot(child);
        } else if (expr instanceof EqualTo) {
            return convertComparison(expr, ConnectorComparison.Operator.EQ, table);
        } else if (expr instanceof GreaterThan) {
            return convertComparison(expr, ConnectorComparison.Operator.GT, table);
        } else if (expr instanceof GreaterThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.GE, table);
        } else if (expr instanceof LessThan) {
            return convertComparison(expr, ConnectorComparison.Operator.LT, table);
        } else if (expr instanceof LessThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.LE, table);
        } else if (expr instanceof InPredicate) {
            return convertIn((InPredicate) expr, table);
        } else if (expr instanceof IsNull) {
            return convertIsNull((IsNull) expr, table);
        } else if (expr instanceof Between) {
            return convertBetween((Between) expr, table);
        }
        return null;
    }

    // AND/OR are all-or-nothing: a single unconvertible child collapses the whole node to null (fail-loud).
    private static ConnectorExpression convertAnd(And and, ExternalTable table) throws AnalysisException {
        List<ConnectorExpression> conjuncts = new ArrayList<>();
        if (!flattenAnd(and, table, conjuncts)) {
            return null;
        }
        return conjuncts.size() == 1 ? conjuncts.get(0) : new ConnectorAnd(conjuncts);
    }

    private static boolean flattenAnd(Expression expr, ExternalTable table, List<ConnectorExpression> out)
            throws AnalysisException {
        if (expr instanceof And) {
            for (Expression child : expr.children()) {
                if (!flattenAnd(child, table, out)) {
                    return false;
                }
            }
            return true;
        }
        ConnectorExpression c = convertNode(expr, table);
        if (c == null) {
            return false;
        }
        out.add(c);
        return true;
    }

    private static ConnectorExpression convertOr(Or or, ExternalTable table) throws AnalysisException {
        List<ConnectorExpression> disjuncts = new ArrayList<>();
        if (!flattenOr(or, table, disjuncts)) {
            return null;
        }
        return disjuncts.size() == 1 ? disjuncts.get(0) : new ConnectorOr(disjuncts);
    }

    private static boolean flattenOr(Expression expr, ExternalTable table, List<ConnectorExpression> out)
            throws AnalysisException {
        if (expr instanceof Or) {
            for (Expression child : expr.children()) {
                if (!flattenOr(child, table, out)) {
                    return false;
                }
            }
            return true;
        }
        ConnectorExpression c = convertNode(expr, table);
        if (c == null) {
            return false;
        }
        out.add(c);
        return true;
    }

    private static ConnectorExpression convertComparison(Expression cmp, ConnectorComparison.Operator op,
            ExternalTable table) throws AnalysisException {
        Expression left = cmp.child(0);
        Expression right = cmp.child(1);
        Slot slot;
        Literal literal;
        if (left instanceof Slot && right instanceof Literal) {
            slot = (Slot) left;
            literal = (Literal) right;
        } else if (left instanceof Literal && right instanceof Slot) {
            slot = (Slot) right;
            literal = (Literal) left;
        } else {
            return null;
        }
        ConnectorExpression litExpr = convertLiteral(literal);
        if (litExpr == null) {
            return null;
        }
        return new ConnectorComparison(op, columnRef(slot, table), litExpr);
    }

    private static ConnectorExpression convertIn(InPredicate in, ExternalTable table) throws AnalysisException {
        if (!(in.child(0) instanceof Slot)) {
            return null;
        }
        List<ConnectorExpression> inList = new ArrayList<>();
        for (int i = 1; i < in.children().size(); i++) {
            Expression child = in.child(i);
            if (!(child instanceof Literal)) {
                return null;
            }
            ConnectorExpression lit = convertLiteral((Literal) child);
            if (lit == null) {
                return null;
            }
            inList.add(lit);
        }
        return new ConnectorIn(columnRef((Slot) in.child(0), table), inList, false);
    }

    private static ConnectorExpression convertIsNull(IsNull isNull, ExternalTable table) throws AnalysisException {
        if (!(isNull.child() instanceof Slot)) {
            return null;
        }
        return new ConnectorIsNull(columnRef((Slot) isNull.child(), table), false);
    }

    private static ConnectorExpression convertBetween(Between between, ExternalTable table)
            throws AnalysisException {
        Expression compareExpr = between.getCompareExpr();
        Expression lower = between.getLowerBound();
        Expression upper = between.getUpperBound();
        if (!(compareExpr instanceof Slot) || !(lower instanceof Literal) || !(upper instanceof Literal)) {
            return null;
        }
        ConnectorExpression lo = convertLiteral((Literal) lower);
        ConnectorExpression hi = convertLiteral((Literal) upper);
        if (lo == null || hi == null) {
            return null;
        }
        return new ConnectorBetween(columnRef((Slot) compareExpr, table), lo, hi);
    }

    // Resolve the column name (handling the unbound parser form: a single name-part UnboundSlot, like the
    // legacy IcebergNereidsUtils.extractColumnName) and its type from the table schema. Fail-loud on a
    // multi-part reference or an unknown column (a name-based silent drop would widen the rewrite).
    private static ConnectorColumnRef columnRef(Slot slot, ExternalTable table) throws AnalysisException {
        String name;
        if (slot instanceof SlotReference) {
            name = ((SlotReference) slot).getName();
        } else if (slot instanceof UnboundSlot) {
            List<String> parts = ((UnboundSlot) slot).getNameParts();
            if (parts.size() != 1) {
                throw new AnalysisException(
                        "WHERE column reference must be a single column name, but got: " + parts);
            }
            name = parts.get(0);
        } else {
            throw new AnalysisException("Unsupported column reference in WHERE: " + slot.getClass().getName());
        }
        Column column = table.getColumn(name);
        if (column == null) {
            throw new AnalysisException("Column not found in table " + table.getName() + ": " + name);
        }
        ConnectorType type = ExprToConnectorExpressionConverter.typeToConnectorType(column.getType());
        return new ConnectorColumnRef(column.getName(), type);
    }

    // Route literals through the analyzed-plan-side converter (Expr -> ConnectorExpression) so the neutral type
    // token + value are byte-identical to the scan / conflict paths. toLegacyLiteral() is a pure transformation.
    private static ConnectorExpression convertLiteral(Literal literal) {
        try {
            return ExprToConnectorExpressionConverter.convert(literal.toLegacyLiteral());
        } catch (Exception e) {
            return null;
        }
    }
}
