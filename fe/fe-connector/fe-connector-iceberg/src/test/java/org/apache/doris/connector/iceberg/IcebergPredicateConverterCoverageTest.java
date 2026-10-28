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
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergPredicateConverterCoverageTest {
    private static final List<ConnectorComparison.Operator> OPERATORS = Arrays.asList(
            ConnectorComparison.Operator.EQ, ConnectorComparison.Operator.LT, ConnectorComparison.Operator.LE,
            ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE);

    @Test
    public void integralPredicatesPreserveOrderingAtNarrowingAndLongBoundaries() {
        long[] literals = {Long.MIN_VALUE, (long) Integer.MIN_VALUE - 1, Integer.MIN_VALUE, -1, 0, 1,
                Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1, 9007199254740993L, Long.MAX_VALUE};
        for (Type type : Arrays.asList(Types.IntegerType.get(), Types.LongType.get())) {
            List<? extends Number> rows = type.typeId() == Type.TypeID.INTEGER
                    ? Arrays.asList(Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE)
                    : Arrays.asList(Long.MIN_VALUE, (long) Integer.MIN_VALUE, -1L, 0L, 1L,
                            (long) Integer.MAX_VALUE, 9007199254740993L, Long.MAX_VALUE);
            Schema schema = schema(type);
            for (IcebergPredicateConverter.Mode mode : IcebergPredicateConverter.Mode.values()) {
                for (long literal : literals) {
                    for (ConnectorComparison.Operator operator : OPERATORS) {
                        Evaluator evaluator = evaluator(schema, mode, operator,
                                new ConnectorLiteral(ConnectorType.of("BIGINT"), literal));
                        for (Number row : rows) {
                            Assertions.assertEquals(matches(operator, Long.compare(row.longValue(), literal)),
                                    evaluator.eval(new SingleValueRow(row)),
                                    mode + " " + type + " " + row + " " + operator + " " + literal);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void decimalPredicatesPreservePrecisionAndEquivalentLiteralScales() {
        Schema schema = schema(Types.DecimalType.of(38, 18));
        List<BigDecimal> values = Arrays.asList(
                new BigDecimal("-99999999999999999999.999999999999999999"),
                new BigDecimal("-0.000000000000000001"),
                new BigDecimal("0.000000000000000000"),
                new BigDecimal("0.000000000000000001"),
                new BigDecimal("99999999999999999999.999999999999999999"));
        for (IcebergPredicateConverter.Mode mode : IcebergPredicateConverter.Mode.values()) {
            for (BigDecimal value : values) {
                for (BigDecimal literal : Arrays.asList(value, value.setScale(20))) {
                    for (ConnectorComparison.Operator operator : OPERATORS) {
                        Evaluator evaluator = evaluator(schema, mode, operator,
                                new ConnectorLiteral(ConnectorType.of("DECIMAL256", 40, literal.scale()), literal));
                        for (BigDecimal row : values) {
                            Assertions.assertEquals(matches(operator, row.compareTo(literal)),
                                    evaluator.eval(new SingleValueRow(row)),
                                    mode + " " + row + " " + operator + " " + literal);
                        }
                    }
                }
            }
        }
    }

    @Test
    @Disabled("Known issue: stripping a DECIMAL-to-DOUBLE cast can prune matching rows and files.")
    public void decimalToDoubleComparisonMustNotPruneMatchingRows() {
        Types.DecimalType type = Types.DecimalType.of(38, 0);
        Schema schema = schema(type);
        BigDecimal row = new BigDecimal("9007199254740993");
        double literal = 9007199254740992d;
        Assertions.assertTrue(row.doubleValue() == literal);
        // The engine removes the column's DOUBLE cast; pruning must preserve its many-to-one equality.
        ConnectorComparison comparison = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("value", ConnectorType.of("DECIMALV3", 38, 0)),
                new ConnectorLiteral(ConnectorType.of("DOUBLE"), literal));
        List<Expression> converted = new IcebergPredicateConverter(schema, ZoneOffset.UTC).convert(comparison);
        Map<Integer, ByteBuffer> bounds = Collections.singletonMap(1, Conversions.toByteBuffer(type, row));
        Metrics metrics = new Metrics(1L, null, Collections.singletonMap(1, 1L),
                Collections.singletonMap(1, 0L), null, bounds, bounds);
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("data/decimal-double.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L).withMetrics(metrics).build();
        // Dropping the predicate is safe; every predicate that is pushed must retain this matching file.
        for (Expression predicate : converted) {
            Assertions.assertAll(
                    () -> Assertions.assertTrue(new Evaluator(schema.asStruct(), predicate)
                            .eval(new SingleValueRow(row)), "a matching row must survive pruning"),
                    () -> Assertions.assertTrue(new InclusiveMetricsEvaluator(schema, predicate).eval(file),
                            "a file containing a matching row must survive pruning"));
        }
    }

    @Test
    public void lineageColumnsNeverBecomeFilePruningPredicates() {
        for (String name : Arrays.asList("_row_id", "_ROW_ID", "_last_updated_sequence_number",
                "_LAST_UPDATED_SEQUENCE_NUMBER")) {
            Schema schema = new Schema(Types.NestedField.optional(1, name, Types.LongType.get()));
            ConnectorColumnRef column = new ConnectorColumnRef(name, ConnectorType.of("BIGINT"));
            for (IcebergPredicateConverter.Mode mode : IcebergPredicateConverter.Mode.values()) {
                IcebergPredicateConverter converter = new IcebergPredicateConverter(schema, ZoneOffset.UTC, mode);
                for (ConnectorComparison.Operator operator : ConnectorComparison.Operator.values()) {
                    Assertions.assertTrue(converter.convert(new ConnectorComparison(
                            operator, column, ConnectorLiteral.ofLong(1L))).isEmpty(), mode + " " + operator);
                }
                Assertions.assertTrue(converter.convert(new ConnectorIn(column,
                        Collections.singletonList(ConnectorLiteral.ofLong(1L)), false)).isEmpty());
                Assertions.assertTrue(converter.convert(new ConnectorComparison(
                        ConnectorComparison.Operator.EQ_FOR_NULL, column,
                        ConnectorLiteral.ofNull(ConnectorType.of("BIGINT")))).isEmpty());
            }
        }
    }

    private static Schema schema(Type type) {
        return new Schema(Types.NestedField.optional(1, "value", type));
    }

    private static Evaluator evaluator(Schema schema, IcebergPredicateConverter.Mode mode,
            ConnectorComparison.Operator operator, ConnectorLiteral literal) {
        ConnectorComparison comparison = new ConnectorComparison(operator,
                new ConnectorColumnRef("value", ConnectorType.of("UNKNOWN")), literal);
        List<Expression> converted = new IcebergPredicateConverter(schema, ZoneOffset.UTC, mode).convert(comparison);
        Assertions.assertEquals(1, converted.size(), mode + " " + comparison);
        return new Evaluator(schema.asStruct(), converted.get(0));
    }

    private static boolean matches(ConnectorComparison.Operator operator, int comparison) {
        switch (operator) {
            case EQ:
                return comparison == 0;
            case LT:
                return comparison < 0;
            case LE:
                return comparison <= 0;
            case GT:
                return comparison > 0;
            case GE:
                return comparison >= 0;
            default:
                throw new IllegalArgumentException(operator.toString());
        }
    }

    private static final class SingleValueRow implements StructLike {
        private final Object value;

        private SingleValueRow(Object value) {
            this.value = value;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public <T> T get(int position, Class<T> javaClass) {
            Assertions.assertEquals(0, position);
            return javaClass.cast(value);
        }

        @Override
        public <T> void set(int position, T value) {
            throw new UnsupportedOperationException();
        }
    }
}
