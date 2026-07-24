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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.table.optimizer.predicate.Attribute;
import com.aliyun.odps.table.optimizer.predicate.BinaryPredicate;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.RawPredicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts {@link ConnectorExpression} trees to ODPS SDK {@link Predicate} objects.
 *
 * <p>Ported from {@code MaxComputeScanNode.convertExprToOdpsPredicate} in fe-core.</p>
 */
public class MaxComputePredicateConverter {
    private static final Logger LOG = LogManager.getLogger(MaxComputePredicateConverter.class);

    static final DateTimeFormatter DATETIME_3_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static final DateTimeFormatter DATETIME_6_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final ZoneId UTC = ZoneId.of("UTC");

    private final Map<String, OdpsType> columnTypeMap;
    private final boolean dateTimePushDown;
    private final String sourceTimeZoneId;

    /**
     * @param columnTypeMap mapping from column name to ODPS type
     * @param dateTimePushDown whether DATETIME/TIMESTAMP predicate push down is enabled
     * @param sourceTimeZoneId the session time zone id (e.g. "Asia/Shanghai"), kept as the raw
     *        string and parsed lazily — only when a DATETIME/TIMESTAMP literal is actually
     *        converted, inside {@link #convert}'s catch. This matters because Doris accepts and
     *        stores some zone ids verbatim that {@link ZoneId#of(String)} rejects (e.g. "CST",
     *        which Doris maps to +08:00 via its own alias map); parsing eagerly would throw out of
     *        query planning, whereas lazy parsing degrades the predicate to
     *        {@link Predicate#NO_PREDICATE} — mirroring legacy {@code MaxComputeScanNode}'s
     *        per-conjunct catch (a non-datetime predicate under such a session still pushes down).
     */
    public MaxComputePredicateConverter(Map<String, OdpsType> columnTypeMap,
            boolean dateTimePushDown, String sourceTimeZoneId) {
        this.columnTypeMap = columnTypeMap;
        this.dateTimePushDown = dateTimePushDown;
        this.sourceTimeZoneId = sourceTimeZoneId;
    }

    /**
     * Convert a ConnectorExpression to an ODPS Predicate.
     * Returns {@link Predicate#NO_PREDICATE} if the expression cannot be converted.
     */
    public Predicate convert(ConnectorExpression expr) {
        if (expr == null) {
            return Predicate.NO_PREDICATE;
        }
        // Top-level conjunction: convert each conjunct independently and AND the survivors, so one
        // unconvertible conjunct doesn't sink the whole filter. This is only safe at the root (a positive,
        // monotone position): dropping a conjunct from the root AND yields a superset that BE re-filters.
        // OR (dropping a disjunct is a subset -> loses rows), NOT, and nested AND are converted whole by
        // convertOne(); partially dropping inside them could change the predicate's meaning.
        if (expr instanceof ConnectorAnd) {
            List<Predicate> survivors = new ArrayList<>();
            for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
                Predicate p = convertOne(conjunct);
                if (p != Predicate.NO_PREDICATE) {
                    survivors.add(p);
                }
            }
            if (survivors.isEmpty()) {
                return Predicate.NO_PREDICATE;
            }
            return survivors.size() == 1
                    ? survivors.get(0)
                    : new CompoundPredicate(CompoundPredicate.Operator.AND, survivors);
        }
        return convertOne(expr);
    }

    private Predicate convertOne(ConnectorExpression expr) {
        try {
            return doConvert(expr);
        } catch (Exception e) {
            LOG.warn("Failed to convert expression to ODPS predicate: {}", e.getMessage());
            return Predicate.NO_PREDICATE;
        }
    }

    private Predicate doConvert(ConnectorExpression expr) {
        if (expr instanceof ConnectorAnd) {
            return convertAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return convertOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorNot) {
            return convertNot((ConnectorNot) expr);
        } else if (expr instanceof ConnectorComparison) {
            return convertComparison((ConnectorComparison) expr);
        } else if (expr instanceof ConnectorIn) {
            return convertIn((ConnectorIn) expr);
        } else if (expr instanceof ConnectorIsNull) {
            return convertIsNull((ConnectorIsNull) expr);
        }
        throw new UnsupportedOperationException(
                "Cannot convert expression type: " + expr.getClass().getSimpleName());
    }

    private Predicate convertAnd(ConnectorAnd and) {
        List<Predicate> children = new ArrayList<>();
        for (ConnectorExpression child : and.getConjuncts()) {
            children.add(doConvert(child));
        }
        return new CompoundPredicate(CompoundPredicate.Operator.AND, children);
    }

    private Predicate convertOr(ConnectorOr or) {
        List<Predicate> children = new ArrayList<>();
        for (ConnectorExpression child : or.getDisjuncts()) {
            children.add(doConvert(child));
        }
        return new CompoundPredicate(CompoundPredicate.Operator.OR, children);
    }

    private Predicate convertNot(ConnectorNot not) {
        List<Predicate> children = new ArrayList<>();
        children.add(doConvert(not.getOperand()));
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, children);
    }

    private Predicate convertComparison(ConnectorComparison cmp) {
        String columnName = extractColumnName(cmp.getLeft());
        String value = formatLiteralValue(columnName, cmp.getRight());

        // Source the operator symbol from the ODPS SDK's own BinaryPredicate.Operator description
        // rather than hand-writing it, mirroring legacy MaxComputeScanNode.convertExprToOdpsPredicate.
        // The SDK is the authority for what ODPS accepts (EQUALS -> "="); a hand-written table let the
        // EQ entry drift to Java's "==", which MaxCompute (like SQL) does not accept -> pushdown lost.
        // EQ_FOR_NULL ("<=>") has no ODPS BinaryPredicate equivalent, so it (and any future operator)
        // falls through to default -> throw -> NO_PREDICATE (BE re-filters), matching legacy's skip.
        BinaryPredicate.Operator odpsOp;
        switch (cmp.getOperator()) {
            case EQ:
                odpsOp = BinaryPredicate.Operator.EQUALS;
                break;
            case NE:
                odpsOp = BinaryPredicate.Operator.NOT_EQUALS;
                break;
            case LT:
                odpsOp = BinaryPredicate.Operator.LESS_THAN;
                break;
            case LE:
                odpsOp = BinaryPredicate.Operator.LESS_THAN_OR_EQUAL;
                break;
            case GT:
                odpsOp = BinaryPredicate.Operator.GREATER_THAN;
                break;
            case GE:
                odpsOp = BinaryPredicate.Operator.GREATER_THAN_OR_EQUAL;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + cmp.getOperator());
        }
        return new RawPredicate(columnName + " " + odpsOp.getDescription() + " " + value);
    }

    private Predicate convertIn(ConnectorIn in) {
        String columnName = extractColumnName(in.getValue());
        String opDesc = in.isNegated() ? "NOT IN" : "IN";

        StringBuilder sb = new StringBuilder();
        sb.append(columnName).append(" ").append(opDesc).append(" (");
        List<ConnectorExpression> values = in.getInList();
        for (int i = 0; i < values.size(); i++) {
            sb.append(formatLiteralValue(columnName, values.get(i)));
            if (i < values.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(" )");
        return new RawPredicate(sb.toString());
    }

    private Predicate convertIsNull(ConnectorIsNull isNull) {
        String columnName = extractColumnName(isNull.getOperand());
        UnaryPredicate.Operator op = isNull.isNegated()
                ? UnaryPredicate.Operator.NOT_NULL
                : UnaryPredicate.Operator.IS_NULL;
        return new UnaryPredicate(op, new Attribute(columnName));
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof ConnectorColumnRef) {
            return ((ConnectorColumnRef) expr).getColumnName();
        }
        throw new UnsupportedOperationException(
                "Expected column reference, got: " + expr.getClass().getSimpleName());
    }

    private String formatLiteralValue(String columnName, ConnectorExpression expr) {
        if (!(expr instanceof ConnectorLiteral)) {
            throw new UnsupportedOperationException(
                    "Expected literal, got: " + expr.getClass().getSimpleName());
        }
        ConnectorLiteral literal = (ConnectorLiteral) expr;
        String rawValue = String.valueOf(literal.getValue());

        OdpsType odpsType = columnTypeMap.get(columnName);
        if (odpsType == null) {
            // Column not in the table schema: mirror legacy MaxComputeScanNode's
            // containsKey guard (throw AnalysisException -> caller drops the predicate).
            // Throwing here degrades the filter to NO_PREDICATE via convert()'s catch,
            // so we never push down a malformed predicate on an unknown column.
            throw new UnsupportedOperationException(
                    "Cannot push down predicate on unknown column: " + columnName);
        }

        switch (odpsType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return " " + rawValue + " ";

            case STRING:
            case CHAR:
            case VARCHAR:
                return " \"" + rawValue + "\" ";

            case DATE:
                return " \"" + rawValue + "\" ";

            case DATETIME:
                if (dateTimePushDown) {
                    return " \"" + formatDateTimeLiteral(
                            literal.getValue(), DATETIME_3_FORMATTER, true) + "\" ";
                }
                break;

            case TIMESTAMP:
                if (dateTimePushDown) {
                    return " \"" + formatDateTimeLiteral(
                            literal.getValue(), DATETIME_6_FORMATTER, true) + "\" ";
                }
                break;

            case TIMESTAMP_NTZ:
                if (dateTimePushDown) {
                    // TIMESTAMP_NTZ carries no timezone: mirror legacy
                    // MaxComputeScanNode:585-592 (getStringValue with NO convertDateTimezone).
                    return " \"" + formatDateTimeLiteral(
                            literal.getValue(), DATETIME_6_FORMATTER, false) + "\" ";
                }
                break;

            default:
                break;
        }
        throw new UnsupportedOperationException(
                "Cannot push down ODPS type: " + odpsType + " for column " + columnName);
    }

    /**
     * Formats a DATETIME/TIMESTAMP/TIMESTAMP_NTZ literal into the ODPS predicate string.
     *
     * <p>The {@code value} is the {@link LocalDateTime} produced by fe-core's
     * {@code ExprToConnectorExpressionConverter.convertDateLiteral} (already at the bound
     * predicate's scale, with nanos = microsecond * 1000). It is formatted directly with
     * {@code formatter} (space-separated, fixed precision: DATETIME {@code .SSS},
     * TIMESTAMP/TIMESTAMP_NTZ {@code .SSSSSS}), reproducing legacy
     * {@code MaxComputeScanNode.convertLiteralToOdpsValues}'s
     * {@code DateLiteral.getStringValue(DatetimeV2Type(3|6))}.</p>
     *
     * <p>Formatting the {@code LocalDateTime} directly avoids the previous defect where
     * {@code String.valueOf(value)} emitted {@link LocalDateTime#toString()}'s 'T'-separated,
     * variable-precision form (e.g. {@code "2023-02-02T00:00"}) — which the space-separated
     * formatter could not parse (whole predicate tree dropped to {@code NO_PREDICATE}) or, on
     * the UTC short-circuit, was pushed malformed to ODPS.</p>
     *
     * @param convertTimeZone {@code true} for DATETIME/TIMESTAMP (legacy converts the session
     *        {@code sourceTimeZone} to UTC, short-circuiting when already UTC); {@code false}
     *        for TIMESTAMP_NTZ (legacy does not convert)
     */
    private String formatDateTimeLiteral(Object value, DateTimeFormatter formatter,
            boolean convertTimeZone) {
        if (!(value instanceof LocalDateTime)) {
            throw new UnsupportedOperationException(
                    "Expected LocalDateTime for datetime predicate, got: "
                            + (value == null ? "null" : value.getClass().getSimpleName()));
        }
        LocalDateTime localDateTime = (LocalDateTime) value;
        if (convertTimeZone) {
            // Parse the session zone here (inside convert()'s catch) rather than eagerly at
            // construction: a Doris-valid-but-ZoneId-invalid id (e.g. "CST") then degrades this
            // predicate to NO_PREDICATE instead of throwing out of query planning.
            ZoneId sourceTimeZone = ZoneId.of(sourceTimeZoneId);
            if (!sourceTimeZone.equals(UTC)) {
                localDateTime = localDateTime.atZone(sourceTimeZone)
                        .withZoneSameInstant(UTC).toLocalDateTime();
            }
        }
        return localDateTime.format(formatter);
    }
}
