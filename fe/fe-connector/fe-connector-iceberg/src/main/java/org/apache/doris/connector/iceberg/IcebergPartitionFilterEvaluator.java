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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Per-file proof that an entire static predicate is guaranteed by partition metadata. */
final class IcebergPartitionFilterEvaluator implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MIN_DATE_DAYS = Math.toIntExact(LocalDate.of(1, 1, 1).toEpochDay());
    private static final int MAX_DATE_DAYS = Math.toIntExact(LocalDate.of(9999, 12, 31).toEpochDay());
    private static final long MIN_TIMESTAMP_MICROS =
            LocalDateTime.of(1, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    private static final long MAX_TIMESTAMP_MICROS =
            LocalDateTime.of(9999, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 999_999L;

    private final Schema schema;
    private final PartitionSpec spec;
    private final PartitionData partition;
    private final ZoneId zone;
    private transient Map<Integer, ConnectorExpression> cachedConjuncts;
    private transient Set<Integer> cachedEnforced;
    private transient boolean rebound;
    private transient PartitionSpec reboundSpec;

    IcebergPartitionFilterEvaluator(Schema schema, PartitionSpec spec, PartitionData partition, ZoneId zone) {
        this.schema = schema;
        this.partition = partition.copy();
        this.zone = zone;
        this.spec = spec;
    }

    private PartitionSpec reboundSpec() {
        if (rebound) {
            return reboundSpec;
        }
        // Partition evolution retains old schemas and names. Rebind by field id only when the
        // source types still agree; a DATE partition must not prove a promoted timestamp bound.
        boolean compatible = spec.fields().stream().allMatch(field ->
                schema.findType(field.sourceId()) != null
                        && schema.findType(field.sourceId()).equals(spec.schema().findType(field.sourceId())));
        reboundSpec = compatible ? PartitionSpecParser.fromJson(schema, PartitionSpecParser.toJson(spec)) : null;
        rebound = true;
        return reboundSpec;
    }

    synchronized Set<Integer> enforced(Map<Integer, ConnectorExpression> conjuncts) {
        if (spec.isUnpartitioned() || conjuncts.isEmpty()) {
            return Collections.emptySet();
        }
        if (conjuncts.equals(cachedConjuncts)) {
            return cachedEnforced;
        }
        Set<Integer> result = new LinkedHashSet<>();
        for (Map.Entry<Integer, ConnectorExpression> entry : conjuncts.entrySet()) {
            Expression predicate = exactPredicate(entry.getValue());
            if (predicate != null && reboundSpec() != null && ResidualEvaluator.of(reboundSpec, predicate, true)
                    .residualFor(partition).op() == Expression.Operation.TRUE) {
                result.add(entry.getKey());
            }
        }
        // The engine supplies one immutable candidate map shared by all files. Retaining that
        // snapshot avoids a per-file copy of every query predicate on large manifest plans.
        cachedConjuncts = conjuncts;
        cachedEnforced = Collections.unmodifiableSet(result);
        return cachedEnforced;
    }

    private Expression exactPredicate(ConnectorExpression expression) {
        ConnectorColumnRef column;
        if (expression instanceof ConnectorComparison) {
            ConnectorComparison comparison = (ConnectorComparison) expression;
            if (!(comparison.getLeft() instanceof ConnectorColumnRef)
                    || !(comparison.getRight() instanceof ConnectorLiteral)) {
                return null;
            }
            column = (ConnectorColumnRef) comparison.getLeft();
            ConnectorLiteral literal = (ConnectorLiteral) comparison.getRight();
            if (literal.isNull()) {
                return exactColumn(column) && comparison.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL
                        ? Expressions.isNull(column.getColumnName()) : null;
            }
            if (!exactLiteral(column, literal)) {
                return null;
            }
        } else if (expression instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expression;
            if (!(in.getValue() instanceof ConnectorColumnRef) || in.getInList().isEmpty()) {
                return null;
            }
            column = (ConnectorColumnRef) in.getValue();
            // NULL members and coercions need SQL three-valued semantics, not a pruning
            // approximation. For example, NOT IN (1, NULL) never evaluates to TRUE.
            if (in.getInList().stream().anyMatch(value -> !(value instanceof ConnectorLiteral)
                    || ((ConnectorLiteral) value).isNull()
                    || !exactLiteral(column, (ConnectorLiteral) value))) {
                return null;
            }
        } else if (expression instanceof ConnectorIsNull) {
            ConnectorIsNull isNull = (ConnectorIsNull) expression;
            if (!(isNull.getOperand() instanceof ConnectorColumnRef)) {
                return null;
            }
            column = (ConnectorColumnRef) isNull.getOperand();
            if (!exactColumn(column)) {
                return null;
            }
            return isNull.isNegated() ? withNonNullDomain(column, Expressions.notNull(column.getColumnName()))
                    : Expressions.isNull(column.getColumnName());
        } else {
            // SCAN conversion can widen AND, NOT and unsupported leaves. Such an expression
            // is useful for pruning but cannot prove the original row filter redundant.
            return null;
        }
        if (!exactColumn(column)) {
            return null;
        }
        Types.NestedField source = schema.findField(column.getColumnName());
        // Iceberg numeric truncate and its strict projection can overflow at signed minima,
        // for both partition values and predicate bounds. A TRUE residual alone cannot prove
        // these value filters redundant; keep them until wraparound can be excluded exactly.
        if ((source.type().typeId() == Type.TypeID.INTEGER || source.type().typeId() == Type.TypeID.LONG)
                && spec.fields().stream().anyMatch(field -> field.sourceId() == source.fieldId()
                        && field.transform().toString().startsWith("truncate["))) {
            return null;
        }
        List<Expression> converted = new IcebergPredicateConverter(schema, zone).convert(expression);
        if (converted.size() != 1) {
            return null;
        }
        // Iceberg's negated predicates may match NULL. SQL comparisons and IN/NOT IN with
        // non-NULL literals do not; retain that distinction in the proof, e.g. id != 1.
        return withNonNullDomain(column, converted.get(0));
    }

    private Expression withNonNullDomain(ConnectorColumnRef column, Expression predicate) {
        String name = column.getColumnName();
        Expression domain = Expressions.notNull(name);
        Type type = schema.findType(name);
        // Iceberg permits years outside Doris's date/time range. A partition proof must also
        // guarantee representability, otherwise eliding a filter-only column hides a decode
        // error (or changes its NULL result), e.g. day(ts) in year 12000 and ts >= year 2000.
        if (type.typeId() == Type.TypeID.DATE) {
            domain = Expressions.and(domain, Expressions.and(
                    Expressions.greaterThanOrEqual(name, MIN_DATE_DAYS),
                    Expressions.lessThanOrEqual(name, MAX_DATE_DAYS)));
        } else if (type.typeId() == Type.TypeID.TIMESTAMP) {
            domain = Expressions.and(domain, Expressions.and(
                    Expressions.greaterThanOrEqual(name, MIN_TIMESTAMP_MICROS),
                    Expressions.lessThanOrEqual(name, MAX_TIMESTAMP_MICROS)));
        }
        return Expressions.and(domain, predicate);
    }

    private boolean exactColumn(ConnectorColumnRef column) {
        Types.NestedField field = schema.findField(column.getColumnName());
        if (field == null || !schema.columns().contains(field)) {
            return false;
        }
        ConnectorType dorisType = column.getType();
        Type type = field.type();
        switch (type.typeId()) {
            case BOOLEAN:
                return "BOOLEAN".equals(dorisType.getTypeName());
            case INTEGER:
                return "INT".equals(dorisType.getTypeName());
            case LONG:
                return "BIGINT".equals(dorisType.getTypeName());
            case DATE:
                return "DATEV2".equals(dorisType.getTypeName());
            case TIMESTAMP:
                String expected = ((Types.TimestampType) type).shouldAdjustToUTC() ? "TIMESTAMPTZ" : "DATETIMEV2";
                return expected.equals(dorisType.getTypeName()) && dorisType.getScale() == 6;
            default:
                // Float/NaN ordering, decimals, string comparisons, precision loss and timezone
                // conversions need separate exactness proofs. Keep their row filters for now.
                return false;
        }
    }

    private boolean exactLiteral(ConnectorColumnRef column, ConnectorLiteral literal) {
        if ("DATETIMEV2".equals(column.getType().getTypeName())
                || "TIMESTAMPTZ".equals(column.getType().getTypeName())) {
            // A whole-second bound can have scale 0 while the source has scale 6. Both are
            // exact microsecond values; TIMESTAMPTZ literals carry UTC fields, not session time.
            return column.getType().getTypeName().equals(literal.getType().getTypeName())
                    && literal.getValue() instanceof LocalDateTime
                    && ((LocalDateTime) literal.getValue()).getNano() % 1000 == 0;
        }
        return column.getType().equals(literal.getType());
    }
}
