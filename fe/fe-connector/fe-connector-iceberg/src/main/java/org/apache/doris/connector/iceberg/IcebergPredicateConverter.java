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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Unbound;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Converts a {@link ConnectorExpression} tree into iceberg {@link Expression} objects for predicate
 * pushdown, mirroring the paimon connector's {@code PaimonPredicateConverter}. This is a self-contained
 * port of fe-core {@code IcebergUtils.convertToIcebergExpr} (the iceberg-side mapping + the
 * {@code extractDorisLiteral} type matrix + the {@code checkConversion} bind-test); it consumes the
 * engine-neutral {@code ConnectorExpression} (produced by fe-core {@code ExprToConnectorExpressionConverter})
 * instead of a Doris {@code Expr}, so it imports only {@code connector.api.pushdown} + {@code org.apache.iceberg}.
 *
 * <p>Handled node set mirrors legacy {@code convertToIcebergExpr} exactly: AND/OR/NOT, comparisons
 * (EQ/NE/LT/LE/GT/GE/EQ_FOR_NULL, column-op-literal only), IN/NOT-IN, and a bare boolean literal
 * (alwaysTrue/alwaysFalse). {@code ConnectorIsNull}/{@code ConnectorLike}/{@code ConnectorBetween}/
 * {@code ConnectorFunctionCall} are dropped (legacy has no such case; IS NULL is still pushed via
 * EQ_FOR_NULL + null literal). Anything that cannot be translated yields {@code null} and is left to BE
 * residual filtering — a safe over-approximation (the filter never removes rows that should match).</p>
 *
 * <p>A second mode — selected by {@code Mode.CONFLICT} (P6.3-T07b) — builds the iceberg expression for
 * write-time optimistic <b>conflict detection</b> (O5-2) instead of scan pushdown. It is a faithful port of
 * legacy {@code IcebergConflictDetectionFilterUtils.convertPredicateToIcebergExpression}, a strictly different
 * matrix: it additionally pushes {@code ConnectorIsNull} / {@code ConnectorBetween}, restricts
 * {@code ConnectorNot} to {@code NOT(IS NULL)}, guards {@code ConnectorOr} to a single column, applies
 * structural/UUID guards, and drops NE / bare booleans. Scan mode (the default) is unchanged.</p>
 *
 * <p>A third mode — {@code Mode.REWRITE} (P6.6-FIX-H9) — lowers the {@code WHERE} of {@code rewrite_data_files}
 * (compaction file scoping). It mirrors legacy {@code IcebergNereidsUtils.convertNereidsToIcebergExpression}:
 * the broad scan matrix (cross-column {@code OR}, any-child {@code NOT}, {@code NE}, {@code IN}) plus the
 * node-emitted {@code IS NULL} / {@code BETWEEN} (which the rewrite-side neutral converter produces directly),
 * but <b>strictly all-or-nothing</b> — a rewrite {@code WHERE} is a user-authored data scope with no downstream
 * re-filter, so any unrepresentable sub-node collapses the whole expression to {@code null} (the rewrite planner
 * then fails loud rather than silently widening the set of files rewritten). Unlike conflict mode it keeps
 * cross-column {@code OR} / {@code NOT(comparison)} / {@code NE} and drops the structural/UUID narrowing.</p>
 */
public class IcebergPredicateConverter {

    private static final Logger LOG = LogManager.getLogger(IcebergPredicateConverter.class);

    // v3 row-lineage metadata columns are never pushable (mirror IcebergUtils.getPushdownField).
    private static final String ICEBERG_ROW_ID_COL = "_row_id";
    private static final String ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL = "_last_updated_sequence_number";

    private final Schema schema;
    private final ZoneId sessionZone;
    // The conversion matrix. SCAN = scan-time pushdown (BE re-filters, so widening is safe). CONFLICT = O5-2
    // write-time optimistic conflict detection (no-missed-conflict, so widening is also safe) -- a port of
    // IcebergConflictDetectionFilterUtils. REWRITE = rewrite_data_files file scoping (no downstream re-filter,
    // so strictly all-or-nothing/precise) -- mirrors IcebergNereidsUtils.convertNereidsToIcebergExpression. The
    // shared leaf helpers (getPushdownField / extractIcebergLiteral / toMicros / checkConversion) are reused.
    private final Mode mode;

    /** Conversion matrix selector. See the field comment and the class javadoc. */
    public enum Mode { SCAN, CONFLICT, REWRITE }

    public IcebergPredicateConverter(Schema schema, ZoneId sessionZone) {
        this(schema, sessionZone, Mode.SCAN);
    }

    // Back-compat boolean constructor (scan vs conflict); REWRITE is selected via the Mode constructor.
    public IcebergPredicateConverter(Schema schema, ZoneId sessionZone, boolean conflictMode) {
        this(schema, sessionZone, conflictMode ? Mode.CONFLICT : Mode.SCAN);
    }

    public IcebergPredicateConverter(Schema schema, ZoneId sessionZone, Mode mode) {
        this.schema = schema;
        this.sessionZone = sessionZone == null ? ZoneOffset.UTC : sessionZone;
        this.mode = mode;
    }

    /**
     * Convert a {@link ConnectorExpression} tree into a list of iceberg {@link Expression}s. Top-level AND
     * nodes are flattened into the list and each top-level conjunct is built + bind-checked independently —
     * mirroring the legacy {@code IcebergScanNode.createTableScan} per-conjunct {@code scan.filter(...)} loop
     * (iceberg ANDs multiple {@code filter()} calls internally). Unconvertible conjuncts are silently dropped.
     */
    public List<Expression> convert(ConnectorExpression expr) {
        List<Expression> results = new ArrayList<>();
        if (expr == null) {
            return results;
        }
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                Expression e = convertSingle(child);
                if (e != null) {
                    results.add(e);
                }
            }
        } else {
            Expression e = convertSingle(expr);
            if (e != null) {
                results.add(e);
            }
        }
        return results;
    }

    /**
     * Build + bind-check one expression, mirroring legacy {@code convertToIcebergExpr} whose every recursive
     * call returns a {@code checkConversion}'d result. Folding the bind-check into the recursion (rather than
     * only at the top) is load-bearing: it makes a nested {@code (a AND b_unbindable) OR c} degrade the bad
     * leaf to {@code a OR c} (legacy parity) instead of carrying an unbindable predicate into the OR.
     */
    private Expression convertSingle(ConnectorExpression expr) {
        return checkConversion(build(expr));
    }

    private Expression build(ConnectorExpression expr) {
        if (expr == null) {
            return null;
        }
        if (mode == Mode.CONFLICT) {
            return buildConflict(expr);
        }
        if (mode == Mode.REWRITE) {
            return buildRewrite(expr);
        }
        if (expr instanceof ConnectorLiteral) {
            return buildBoolLiteral((ConnectorLiteral) expr);
        } else if (expr instanceof ConnectorAnd) {
            return buildAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return buildOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorNot) {
            return buildNot((ConnectorNot) expr);
        } else if (expr instanceof ConnectorComparison) {
            return buildComparison((ConnectorComparison) expr);
        } else if (expr instanceof ConnectorIn) {
            return buildIn((ConnectorIn) expr);
        }
        return null;
    }

    // A bare boolean literal -> alwaysTrue / alwaysFalse (legacy BoolLiteral path); anything else dropped.
    private Expression buildBoolLiteral(ConnectorLiteral literal) {
        Object value = literal.getValue();
        if (value instanceof Boolean) {
            return ((Boolean) value) ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
        }
        return null;
    }

    // AND composition. SCAN/CONFLICT degrade -- drop unbindable arms, keep the pushable subset (widening is safe
    // there). REWRITE is all-or-nothing: a single unconvertible arm collapses the whole AND to null, so the
    // rewrite planner's guard turns it into a hard error rather than silently widening the set of files rewritten.
    private Expression buildAnd(ConnectorAnd and) {
        Expression result = null;
        for (ConnectorExpression child : and.getConjuncts()) {
            Expression c = convertSingle(child);
            if (c == null) {
                if (mode == Mode.REWRITE) {
                    return null;
                }
                continue;
            }
            result = (result == null) ? c : Expressions.and(result, c);
        }
        return result;
    }

    // OR is all-or-nothing: any unpushable disjunct collapses the whole OR (dropping an arm widens results).
    private Expression buildOr(ConnectorOr or) {
        Expression result = null;
        for (ConnectorExpression child : or.getDisjuncts()) {
            Expression c = convertSingle(child);
            if (c == null) {
                return null;
            }
            result = (result == null) ? c : Expressions.or(result, c);
        }
        return result;
    }

    private Expression buildNot(ConnectorNot not) {
        Expression child = convertSingle(not.getOperand());
        return child == null ? null : Expressions.not(child);
    }

    private Expression buildComparison(ConnectorComparison cmp) {
        ConnectorExpression left = cmp.getLeft();
        ConnectorExpression right = cmp.getRight();
        // Column-op-literal only (drop reversed `literal OP col` and col-col). Nereids normalizes comparisons
        // so the column is on the left; dropping the rest is a safe over-approximation.
        if (!(left instanceof ConnectorColumnRef) || !(right instanceof ConnectorLiteral)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) left).getColumnName());
        if (field == null) {
            return null;
        }
        String colName = field.name();
        ConnectorLiteral literal = (ConnectorLiteral) right;
        Object value = extractIcebergLiteral(field.type(), literal);
        if (value == null) {
            // Only EQ_FOR_NULL (col <=> NULL) survives a null value -> IS NULL; everything else is dropped.
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL && literal.isNull()) {
                return Expressions.isNull(colName);
            }
            return null;
        }
        switch (cmp.getOperator()) {
            case EQ:
            case EQ_FOR_NULL:
                return Expressions.equal(colName, value);
            case NE:
                return Expressions.not(Expressions.equal(colName, value));
            case GE:
                return Expressions.greaterThanOrEqual(colName, value);
            case GT:
                return Expressions.greaterThan(colName, value);
            case LE:
                return Expressions.lessThanOrEqual(colName, value);
            case LT:
                return Expressions.lessThan(colName, value);
            default:
                return null;
        }
    }

    private Expression buildIn(ConnectorIn in) {
        if (!(in.getValue() instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) in.getValue()).getColumnName());
        if (field == null) {
            return null;
        }
        String colName = field.name();
        List<Object> values = new ArrayList<>();
        for (ConnectorExpression item : in.getInList()) {
            if (!(item instanceof ConnectorLiteral)) {
                return null;
            }
            Object value = extractIcebergLiteral(field.type(), (ConnectorLiteral) item);
            if (value == null) {
                // A single unconvertible element drops the whole IN/NOT-IN (legacy parity).
                return null;
            }
            values.add(value);
        }
        return in.isNegated() ? Expressions.notIn(colName, values) : Expressions.in(colName, values);
    }

    private Types.NestedField getPushdownField(String colName) {
        if (ICEBERG_ROW_ID_COL.equalsIgnoreCase(colName)
                || ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL.equalsIgnoreCase(colName)) {
            return null;
        }
        return schema.caseInsensitiveFindField(colName);
    }

    /**
     * Port of legacy {@code IcebergUtils.extractDorisLiteral}: maps a literal's plain Java value (carried by
     * {@link ConnectorLiteral}, produced by {@code ExprToConnectorExpressionConverter}) to the iceberg-typed
     * value accepted by {@code Expressions.*}, gated by the (Doris-source-type x iceberg-type) matrix. Returns
     * {@code null} when the pair is not pushable. The Doris source primitive (needed to tell int32 from int64
     * and float from double, both flattened to one Java type) is read from {@link ConnectorLiteral#getType()}.
     */
    private Object extractIcebergLiteral(Type icebergType, ConnectorLiteral literal) {
        if (literal.isNull()) {
            return null;
        }
        Object value = literal.getValue();
        TypeID id = icebergType.typeId();
        if (value instanceof Boolean) {
            switch (id) {
                case BOOLEAN:
                    return value;
                case STRING:
                    // BoolLiteral.getStringValue() == "1"/"0".
                    return ((Boolean) value) ? "1" : "0";
                default:
                    return null;
            }
        } else if (value instanceof LocalDate) {
            LocalDate date = (LocalDate) value;
            switch (id) {
                case STRING:
                case DATE:
                    return date.toString();
                case TIMESTAMP:
                    return toMicros(date.atStartOfDay(), icebergType);
                default:
                    return null;
            }
        } else if (value instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) value;
            switch (id) {
                case STRING:
                case DATE:
                    return dorisDateTimeString(dateTime);
                case TIMESTAMP:
                    return toMicros(dateTime, icebergType);
                default:
                    return null;
            }
        } else if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            switch (id) {
                case DECIMAL:
                    return decimal;
                case STRING:
                    return decimal.toString();
                case FLOAT:
                    // Nereids types an unsuffixed decimal literal (e.g. 90.0) as a BigDecimal, so a FLOAT-column
                    // predicate lands here, not in the Double/Float branch. Mirror that branch: only REWRITE may
                    // push it (file scoping with no downstream re-filter -> the decimal->float narrowing can only
                    // shrink the rewrite set, which is safe); SCAN/CONFLICT omit it to avoid wrongly pruning
                    // matching rows. Restores legacy IcebergNereidsUtils rewrite behaviour (column-type FLOAT).
                    return mode == Mode.REWRITE ? decimal.doubleValue() : null;
                case DOUBLE:
                    return decimal.doubleValue();
                default:
                    return null;
            }
        } else if (value instanceof Double || value instanceof Float) {
            double doubleValue = ((Number) value).doubleValue();
            if (isFloatType(literal)) {
                switch (id) {
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                        return doubleValue;
                    default:
                        return null;
                }
            }
            switch (id) {
                case FLOAT:
                    // Only REWRITE may push a (non-float-typed) double/decimal literal onto a FLOAT column:
                    // it scopes file rewriting with no downstream re-filter, so the double->float narrowing
                    // can only shrink the rewrite set (a file merely goes un-rewritten), which is safe.
                    // SCAN/CONFLICT keep omitting it -- an incorrect prune there would silently drop matching
                    // rows (BE re-filters a widened scan, but cannot recover a wrongly-pruned file). Restores
                    // legacy IcebergNereidsUtils rewrite behaviour (case FLOAT -> floatValue()).
                    return mode == Mode.REWRITE ? doubleValue : null;
                case DOUBLE:
                case DECIMAL:
                    return doubleValue;
                default:
                    return null;
            }
        } else if (value instanceof Long || value instanceof Integer) {
            long longValue = ((Number) value).longValue();
            if (isInteger32(literal)) {
                switch (id) {
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case DECIMAL:
                        return (int) longValue;
                    default:
                        return null;
                }
            }
            switch (id) {
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case TIME:
                case TIMESTAMP:
                case DATE:
                case DECIMAL:
                    return longValue;
                default:
                    return null;
            }
        } else if (value instanceof String) {
            String string = (String) value;
            switch (id) {
                case TIMESTAMP:
                    if (mode == Mode.SCAN) {
                        // Legacy IcebergUtils.extractDorisLiteral returns the raw string for a TIMESTAMP column and
                        // lets the iceberg bind-check accept only a well-formed ISO timestamp; a date-only or
                        // space-separated Doris string is then dropped. Returning the raw string keeps that scan
                        // parity -- a VARCHAR must not push onto a timestamp column any wider than the legacy
                        // IcebergScanNode did. (Normal scans coerce the literal to a DateTimeLiteral -> the
                        // LocalDateTime branch, so this raw-string path is only the uncoerced case.)
                        return string;
                    }
                    // REWRITE / CONFLICT are not type-coerced, so a quoted datetime literal arrives here as a raw
                    // Doris-format string ("yyyy-MM-dd HH:mm:ss[.SSSSSS]", or a date-only string). Iceberg's own
                    // string->timestamp bind expects ISO-8601 (with 'T') and rejects the space-separated form, so
                    // parse it to zone-adjusted epoch micros ourselves (via toMicros, honoring timestamptz),
                    // mirroring legacy IcebergNereidsUtils' string branch -- which both the rewrite planner and the
                    // conflict-detection util (IcebergConflictDetectionFilterUtils) delegate to. Null (drop) when
                    // not a parseable datetime.
                    LocalDateTime parsedTs = parseDorisDateTime(string);
                    return parsedTs == null ? null : toMicros(parsedTs, icebergType);
                case DATE:
                case TIME:
                case STRING:
                case UUID:
                case DECIMAL:
                    return string;
                case INTEGER:
                    try {
                        return Integer.parseInt(string);
                    } catch (Exception e) {
                        return null;
                    }
                case LONG:
                    try {
                        return Long.parseLong(string);
                    } catch (Exception e) {
                        return null;
                    }
                default:
                    return null;
            }
        }
        return null;
    }

    private static boolean isFloatType(ConnectorLiteral literal) {
        return "FLOAT".equals(literal.getType().getTypeName().toUpperCase(Locale.ROOT));
    }

    // Mirror Type.isInteger32Type(): TINYINT / SMALLINT / INT. BIGINT (and anything else) is treated as 64-bit.
    private static boolean isInteger32(ConnectorLiteral literal) {
        String name = literal.getType().getTypeName().toUpperCase(Locale.ROOT);
        return "TINYINT".equals(name) || "SMALLINT".equals(name) || "INT".equals(name);
    }

    // Epoch micros of a wall-clock datetime, interpreted in the session zone for zone-adjusted timestamps
    // (timestamptz) or UTC otherwise (mirrors legacy DateLiteral.getUnixTimestampWithMicroseconds).
    private long toMicros(LocalDateTime dateTime, Type icebergType) {
        ZoneId zone = ((Types.TimestampType) icebergType).shouldAdjustToUTC() ? sessionZone : ZoneOffset.UTC;
        Instant instant = dateTime.atZone(zone).toInstant();
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    }

    // Parse a Doris-format datetime string ("yyyy-MM-dd HH:mm:ss[.fraction]") -- or a date-only string, taken
    // at start-of-day -- to a LocalDateTime, or null when unparseable. Normalizes the space separator to ISO
    // 'T' so java.time's ISO parsers accept the Doris rendering (mirrors legacy IcebergNereidsUtils' string ->
    // timestamp path, which parsed the literal itself rather than handing the raw string to iceberg).
    private static LocalDateTime parseDorisDateTime(String value) {
        String iso = value.trim().replace(' ', 'T');
        try {
            return LocalDateTime.parse(iso);
        } catch (RuntimeException ignored) {
            // not a full datetime -- fall through to date-only
        }
        try {
            return LocalDate.parse(iso).atStartOfDay();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    // Best-effort Doris-style "yyyy-MM-dd HH:mm:ss[.SSSSSS]" (DateLiteral.getStringValue). Only reached for a
    // datetime literal compared to a STRING/DATE iceberg column; the DATE case fails the bind-check and drops.
    private static String dorisDateTimeString(LocalDateTime dateTime) {
        StringBuilder sb = new StringBuilder(26);
        appendPadded(sb, dateTime.getYear(), 4);
        sb.append('-');
        appendPadded(sb, dateTime.getMonthValue(), 2);
        sb.append('-');
        appendPadded(sb, dateTime.getDayOfMonth(), 2);
        sb.append(' ');
        appendPadded(sb, dateTime.getHour(), 2);
        sb.append(':');
        appendPadded(sb, dateTime.getMinute(), 2);
        sb.append(':');
        appendPadded(sb, dateTime.getSecond(), 2);
        int micros = dateTime.getNano() / 1000;
        if (micros > 0) {
            sb.append('.');
            appendPadded(sb, micros, 6);
        }
        return sb.toString();
    }

    private static void appendPadded(StringBuilder sb, int value, int width) {
        String s = Integer.toString(value);
        for (int i = s.length(); i < width; i++) {
            sb.append('0');
        }
        sb.append(s);
    }

    /**
     * Port of legacy {@code IcebergUtils.checkConversion}: validates that an assembled (unbound) expression
     * actually binds to the schema, returning the still-unbound expression on success or {@code null} on
     * failure. AND keeps the bindable arm in SCAN/CONFLICT (widening is safe there) but is all-or-nothing in
     * REWRITE (a half-bindable AND -- e.g. a BETWEEN whose upper bound is a bindable-but-malformed temporal
     * string -- must collapse so the rewrite planner fails loud, never degrade to the surviving arm and silently
     * widen the file set); OR/NOT are all-or-nothing; TRUE/FALSE always pass; leaf predicates are bound
     * (case-sensitive) and dropped if the bind throws (e.g. out-of-range literal).
     */
    private Expression checkConversion(Expression expression) {
        if (expression == null) {
            return null;
        }
        switch (expression.op()) {
            case AND: {
                And andExpr = (And) expression;
                Expression left = checkConversion(andExpr.left());
                Expression right = checkConversion(andExpr.right());
                if (left != null && right != null) {
                    return andExpr;
                } else if (mode == Mode.REWRITE) {
                    // all-or-nothing: a single unbindable arm fails the whole AND (no silent widen for rewrite).
                    return null;
                } else if (left != null) {
                    return left;
                } else if (right != null) {
                    return right;
                } else {
                    return null;
                }
            }
            case OR: {
                Or orExpr = (Or) expression;
                Expression left = checkConversion(orExpr.left());
                Expression right = checkConversion(orExpr.right());
                if (left == null || right == null) {
                    return null;
                }
                return orExpr;
            }
            case NOT: {
                Not notExpr = (Not) expression;
                Expression child = checkConversion(notExpr.child());
                return child == null ? null : notExpr;
            }
            case TRUE:
            case FALSE:
                return expression;
            default:
                if (!(expression instanceof Unbound)) {
                    return null;
                }
                try {
                    ((Unbound<?, ?>) expression).bind(schema.asStruct(), true);
                    return expression;
                } catch (Exception e) {
                    LOG.debug("Failed to check expression: {}", e.getMessage());
                    return null;
                }
        }
    }

    // ====================================================================================================
    // Conflict-mode (O5-2 write-time conflict detection). Port of legacy
    // IcebergConflictDetectionFilterUtils.convertPredicateToIcebergExpression. Reached only when
    // conflictMode == true; the scan dispatch above is untouched. Recursive children flow back through
    // convertSingle -> build -> here, so the conflict matrix applies all the way down.
    // ====================================================================================================

    private Expression buildConflict(ConnectorExpression expr) {
        if (expr instanceof ConnectorAnd) {
            // AND keeps the bindable subset (dropping an arm only widens the conflict filter -> safe).
            return buildAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return buildConflictOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorNot) {
            return buildConflictNot((ConnectorNot) expr);
        } else if (expr instanceof ConnectorIsNull) {
            ConnectorIsNull isNull = (ConnectorIsNull) expr;
            return buildConflictIsNull(isNull.getOperand(), isNull.isNegated());
        } else if (expr instanceof ConnectorIn) {
            return buildConflictIn((ConnectorIn) expr);
        } else if (expr instanceof ConnectorBetween) {
            return buildConflictBetween((ConnectorBetween) expr);
        } else if (expr instanceof ConnectorComparison) {
            return buildConflictComparison((ConnectorComparison) expr);
        }
        // bare boolean literal / LIKE / function call: not in the legacy conflict matrix -> dropped.
        return null;
    }

    // OR is pushed only when every disjunct binds AND all disjuncts reference the same single column (legacy
    // isSameColumnPredicate). A cross-column OR is dropped: pushing it would narrow the filter (missed conflict).
    private Expression buildConflictOr(ConnectorOr or) {
        Expression result = null;
        int commonFieldId = 0;
        boolean haveField = false;
        for (ConnectorExpression child : or.getDisjuncts()) {
            Expression c = convertSingle(child);
            if (c == null) {
                return null;
            }
            Types.NestedField field = resolveConflictField(child);
            if (field == null) {
                return null;
            }
            if (!haveField) {
                commonFieldId = field.fieldId();
                haveField = true;
            } else if (commonFieldId != field.fieldId()) {
                return null;
            }
            result = (result == null) ? c : Expressions.or(result, c);
        }
        return result;
    }

    // NOT is pushed only for NOT(IS NULL) -> not(isNull); legacy restricts NOT to an IS NULL child.
    private Expression buildConflictNot(ConnectorNot not) {
        if (!(not.getOperand() instanceof ConnectorIsNull)) {
            return null;
        }
        ConnectorIsNull inner = (ConnectorIsNull) not.getOperand();
        return buildConflictIsNull(inner.getOperand(), !inner.isNegated());
    }

    private Expression buildConflictIsNull(ConnectorExpression operand, boolean negated) {
        if (!(operand instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) operand).getColumnName());
        if (field == null || isStructural(field.type())) {
            return null;
        }
        Expression isNull = Expressions.isNull(field.name());
        return negated ? Expressions.not(isNull) : isNull;
    }

    private Expression buildConflictIn(ConnectorIn in) {
        if (in.isNegated() || !(in.getValue() instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) in.getValue()).getColumnName());
        if (field == null) {
            return null;
        }
        Type type = field.type();
        if (isStructural(type)) {
            return null;
        }
        boolean hasNull = false;
        List<Object> values = new ArrayList<>();
        for (ConnectorExpression item : in.getInList()) {
            if (!(item instanceof ConnectorLiteral)) {
                return null;
            }
            ConnectorLiteral literal = (ConnectorLiteral) item;
            if (literal.isNull()) {
                hasNull = true;
                continue;
            }
            Object value = extractIcebergLiteral(type, literal);
            if (value == null) {
                // a single unconvertible element drops the whole IN (legacy parity)
                return null;
            }
            values.add(value);
        }
        if (isUuid(type) && !values.isEmpty()) {
            return null;
        }
        Expression valuesExpr = values.isEmpty() ? null : Expressions.in(field.name(), values);
        Expression nullExpr = hasNull ? Expressions.isNull(field.name()) : null;
        return combineOr(nullExpr, valuesExpr);
    }

    private Expression buildConflictBetween(ConnectorBetween between) {
        if (!(between.getValue() instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) between.getValue()).getColumnName());
        if (field == null) {
            return null;
        }
        Type type = field.type();
        if (isStructural(type) || isUuid(type)) {
            return null;
        }
        if (!(between.getLower() instanceof ConnectorLiteral)
                || !(between.getUpper() instanceof ConnectorLiteral)) {
            return null;
        }
        Object lo = extractIcebergLiteral(type, (ConnectorLiteral) between.getLower());
        Object hi = extractIcebergLiteral(type, (ConnectorLiteral) between.getUpper());
        if (lo == null || hi == null) {
            return null;
        }
        String colName = field.name();
        return Expressions.and(
                Expressions.greaterThanOrEqual(colName, lo), Expressions.lessThanOrEqual(colName, hi));
    }

    private Expression buildConflictComparison(ConnectorComparison cmp) {
        if (!(cmp.getLeft() instanceof ConnectorColumnRef) || !(cmp.getRight() instanceof ConnectorLiteral)) {
            // column-op-literal only (the neutral converter normalises the column to the left).
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) cmp.getLeft()).getColumnName());
        if (field == null) {
            return null;
        }
        Type type = field.type();
        if (isStructural(type)) {
            return null;
        }
        String colName = field.name();
        ConnectorLiteral literal = (ConnectorLiteral) cmp.getRight();
        if (isUuid(type)) {
            // UUID is comparable only as `col = NULL` -> IS NULL; every other UUID comparison is dropped.
            return cmp.getOperator() == ConnectorComparison.Operator.EQ && literal.isNull()
                    ? Expressions.isNull(colName) : null;
        }
        if (literal.isNull()) {
            // mirror legacy convertNereidsBinaryPredicate: any of EQ/GT/GE/LT/LE against NULL -> IS NULL.
            switch (cmp.getOperator()) {
                case EQ:
                case GT:
                case GE:
                case LT:
                case LE:
                    return Expressions.isNull(colName);
                default:
                    return null;
            }
        }
        Object value = extractIcebergLiteral(type, literal);
        if (value == null) {
            return null;
        }
        switch (cmp.getOperator()) {
            case EQ:
                return Expressions.equal(colName, value);
            case GT:
                return Expressions.greaterThan(colName, value);
            case GE:
                return Expressions.greaterThanOrEqual(colName, value);
            case LT:
                return Expressions.lessThan(colName, value);
            case LE:
                return Expressions.lessThanOrEqual(colName, value);
            default:
                // NE / EQ_FOR_NULL are not part of the legacy conflict matrix -> dropped.
                return null;
        }
    }

    // Resolve the single iceberg field a sub-expression references (legacy resolveSingleField). Returns null
    // when the sub-expression references zero or multiple distinct columns, or a non-pushable column.
    private Types.NestedField resolveConflictField(ConnectorExpression expr) {
        Set<String> columns = new LinkedHashSet<>();
        collectColumnNames(expr, columns);
        if (columns.size() != 1) {
            return null;
        }
        return getPushdownField(columns.iterator().next());
    }

    private void collectColumnNames(ConnectorExpression expr, Set<String> out) {
        if (expr instanceof ConnectorColumnRef) {
            out.add(((ConnectorColumnRef) expr).getColumnName());
            return;
        }
        for (ConnectorExpression child : expr.getChildren()) {
            collectColumnNames(child, out);
        }
    }

    // ====================================================================================================
    // Rewrite-mode (rewrite_data_files file scoping, P6.6-FIX-H9). Mirrors legacy
    // IcebergNereidsUtils.convertNereidsToIcebergExpression: the broad scan matrix (cross-column OR, any-child
    // NOT, NE, IN) plus the node-emitted IS NULL / BETWEEN, but strictly all-or-nothing (precise or null, never
    // widen) -- a rewrite WHERE has no downstream re-filter, so a dropped node would rewrite MORE files than the
    // user asked. The shared leaves (buildComparison / buildIn / getPushdownField / extractIcebergLiteral /
    // checkConversion) and the already all-or-nothing buildOr / buildNot are reused; only AND (all-or-nothing via
    // buildAnd) and IS NULL / BETWEEN (absent from the scan dispatch) are rewrite-specific.
    // ====================================================================================================

    private Expression buildRewrite(ConnectorExpression expr) {
        if (expr instanceof ConnectorAnd) {
            return buildAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return buildOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorNot) {
            return buildNot((ConnectorNot) expr);
        } else if (expr instanceof ConnectorComparison) {
            return buildComparison((ConnectorComparison) expr);
        } else if (expr instanceof ConnectorIn) {
            return buildIn((ConnectorIn) expr);
        } else if (expr instanceof ConnectorIsNull) {
            return buildRewriteIsNull((ConnectorIsNull) expr);
        } else if (expr instanceof ConnectorBetween) {
            return buildRewriteBetween((ConnectorBetween) expr);
        }
        // bare boolean literal / LIKE / function call: master throws "Unsupported expression type" -> null here,
        // which the planner guard turns into a hard error (never a silent widen).
        return null;
    }

    // IS NULL over a column (legacy convertNereidsToIcebergExpression IS NULL arm). No structural guard -- the
    // bind-check drops a genuinely-unbindable form, mirroring master. IS NOT NULL arrives as Not(IsNull); a
    // negated node is handled defensively all the same.
    private Expression buildRewriteIsNull(ConnectorIsNull isNull) {
        if (!(isNull.getOperand() instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) isNull.getOperand()).getColumnName());
        if (field == null) {
            return null;
        }
        Expression expr = Expressions.isNull(field.name());
        return isNull.isNegated() ? Expressions.not(expr) : expr;
    }

    // BETWEEN col, lo, hi -> col >= lo AND col <= hi (legacy convertNereidsBetween). No structural/UUID guard;
    // extractIcebergLiteral returning null (incl. struct/list/map columns) drops the node, mirroring master.
    private Expression buildRewriteBetween(ConnectorBetween between) {
        if (!(between.getValue() instanceof ConnectorColumnRef)) {
            return null;
        }
        Types.NestedField field = getPushdownField(((ConnectorColumnRef) between.getValue()).getColumnName());
        if (field == null) {
            return null;
        }
        if (!(between.getLower() instanceof ConnectorLiteral)
                || !(between.getUpper() instanceof ConnectorLiteral)) {
            return null;
        }
        Object lo = extractIcebergLiteral(field.type(), (ConnectorLiteral) between.getLower());
        Object hi = extractIcebergLiteral(field.type(), (ConnectorLiteral) between.getUpper());
        if (lo == null || hi == null) {
            return null;
        }
        String colName = field.name();
        return Expressions.and(
                Expressions.greaterThanOrEqual(colName, lo), Expressions.lessThanOrEqual(colName, hi));
    }

    private static Expression combineOr(Expression left, Expression right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Expressions.or(left, right);
    }

    private static boolean isStructural(Type type) {
        return type.isStructType() || type.isListType() || type.isMapType();
    }

    private static boolean isUuid(Type type) {
        return type.typeId() == TypeID.UUID;
    }
}
