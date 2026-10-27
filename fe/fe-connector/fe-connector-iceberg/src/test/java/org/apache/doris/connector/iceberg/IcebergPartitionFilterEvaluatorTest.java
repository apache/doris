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
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class IcebergPartitionFilterEvaluatorTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "id", Types.IntegerType.get()));

    @Test
    void wholeDayBoundsAreEnforcedButWithinDayBoundRemains() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, Math.toIntExact(LocalDate.of(2026, 10, 12).toEpochDay()));
        Map<Integer, ConnectorExpression> predicates = new LinkedHashMap<>();
        predicates.put(3, timestampPredicate(ConnectorComparison.Operator.GE, "2026-10-12T00:00:00"));
        predicates.put(8, timestampPredicate(ConnectorComparison.Operator.LT, "2026-10-13T00:00:00"));
        predicates.put(10, timestampPredicate(ConnectorComparison.Operator.GE, "2026-10-12T13:00:00"));
        Assertions.assertEquals(Set.of(3, 8),
                new IcebergPartitionFilterEvaluator(SCHEMA, spec, partition, ZoneOffset.UTC).enforced(predicates));
    }

    @Test
    void historicalMonthSpecCannotProveADayRange() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, (2026 - 1970) * 12 + 9);
        Map<Integer, ConnectorExpression> predicates = Map.of(
                0, timestampPredicate(ConnectorComparison.Operator.GE, "2026-10-12T00:00:00"),
                1, timestampPredicate(ConnectorComparison.Operator.LT, "2026-10-13T00:00:00"));
        Assertions.assertTrue(new IcebergPartitionFilterEvaluator(SCHEMA, spec, partition, ZoneOffset.UTC)
                .enforced(predicates).isEmpty());
    }

    @Test
    void hourPartitionProvesWholeHourButNotMinutesWithinIt() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("ts").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, Math.toIntExact(LocalDate.of(2026, 10, 12).toEpochDay() * 24 + 13));
        Map<Integer, ConnectorExpression> predicates = Map.of(
                1, timestampPredicate(ConnectorComparison.Operator.GE, "2026-10-12T13:00:00"),
                2, timestampPredicate(ConnectorComparison.Operator.LT, "2026-10-12T14:00:00"),
                3, timestampPredicate(ConnectorComparison.Operator.GE, "2026-10-12T13:30:00"));
        Assertions.assertEquals(Set.of(1, 2), new IcebergPartitionFilterEvaluator(
                SCHEMA, spec, partition, ZoneOffset.UTC).enforced(predicates));
    }

    @Test
    void truncateProvesRangeButBucketDoesNotProveEquality() {
        PartitionSpec truncate = PartitionSpec.builderFor(SCHEMA).truncate("id", 10).build();
        PartitionData values = new PartitionData(truncate.partitionType());
        values.set(0, 20);
        Map<Integer, ConnectorExpression> predicates = Map.of(
                0, intPredicate(ConnectorComparison.Operator.GE, 20),
                1, intPredicate(ConnectorComparison.Operator.LT, 30),
                2, intPredicate(ConnectorComparison.Operator.EQ, 21));
        Assertions.assertEquals(Set.of(0, 1), new IcebergPartitionFilterEvaluator(
                SCHEMA, truncate, values, ZoneOffset.UTC).enforced(predicates));

        PartitionSpec bucket = PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();
        PartitionData bucketValues = new PartitionData(bucket.partitionType());
        bucketValues.set(0, Transforms.<Integer>bucket(16).bind(Types.IntegerType.get()).apply(21));
        Assertions.assertTrue(new IcebergPartitionFilterEvaluator(SCHEMA, bucket, bucketValues, ZoneOffset.UTC)
                .enforced(Map.of(2, intPredicate(ConnectorComparison.Operator.EQ, 21))).isEmpty());
    }

    @Test
    void nullPartitionDoesNotProveNegatedComparisonsOrNotIn() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        ConnectorColumnRef id = new ConnectorColumnRef("id", ConnectorType.of("INT"));
        Map<Integer, ConnectorExpression> predicates = Map.of(
                0, intPredicate(ConnectorComparison.Operator.NE, 1),
                1, new ConnectorIn(id, List.of(ConnectorLiteral.ofInt(1)), true),
                2, new ConnectorIsNull(id, false),
                3, new ConnectorIsNull(id, true));
        Assertions.assertEquals(Set.of(2), new IcebergPartitionFilterEvaluator(
                SCHEMA, spec, partition, ZoneOffset.UTC).enforced(predicates));
    }

    @Test
    void unsupportedConjunctsCannotBorrowAnInclusiveProof() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, 2);
        ConnectorColumnRef id = new ConnectorColumnRef("id", ConnectorType.of("INT"));
        ConnectorExpression nullNotIn = new ConnectorIn(id,
                List.of(ConnectorLiteral.ofInt(1), ConnectorLiteral.ofNull(id.getType())), true);
        Map<Integer, ConnectorExpression> predicates = Map.of(
                0, new ConnectorAnd(List.of(intPredicate(ConnectorComparison.Operator.GE, 1), nullNotIn)),
                1, nullNotIn,
                2, intPredicate(ConnectorComparison.Operator.GE, 1));
        Assertions.assertEquals(Set.of(2), new IcebergPartitionFilterEvaluator(
                SCHEMA, spec, partition, ZoneOffset.UTC).enforced(predicates));
    }

    @Test
    void renamedFieldBindsByIdAndUnpartitionedSpecKeepsFilters() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, 2);
        Schema renamed = new Schema(Types.NestedField.optional(2, "renamed_id", Types.IntegerType.get()));
        ConnectorType type = ConnectorType.of("INT");
        Map<Integer, ConnectorExpression> predicates = Map.of(5, new ConnectorComparison(
                ConnectorComparison.Operator.EQ, new ConnectorColumnRef("renamed_id", type),
                new ConnectorLiteral(type, 2L)));
        Assertions.assertEquals(Set.of(5), new IcebergPartitionFilterEvaluator(
                renamed, spec, partition, ZoneOffset.UTC).enforced(predicates));
        Assertions.assertTrue(new IcebergPartitionFilterEvaluator(renamed, PartitionSpec.unpartitioned(),
                new PartitionData(Types.StructType.of()), ZoneOffset.UTC).enforced(predicates).isEmpty());
    }

    private static ConnectorExpression intPredicate(ConnectorComparison.Operator op, int value) {
        ConnectorType type = ConnectorType.of("INT");
        return new ConnectorComparison(op, new ConnectorColumnRef("id", type),
                new ConnectorLiteral(type, (long) value));
    }

    static ConnectorExpression timestampPredicate(ConnectorComparison.Operator op, String value) {
        ConnectorType type = ConnectorType.of("DATETIMEV2", 0, 6);
        return new ConnectorComparison(op, new ConnectorColumnRef("ts", type),
                new ConnectorLiteral(type, LocalDateTime.parse(value)));
    }

    @Test
    void zonedTimestampUsesUtcLiteralFieldsInEverySessionZone() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, Math.toIntExact(LocalDate.of(2021, 11, 7).toEpochDay()));
        ConnectorColumnRef ts = new ConnectorColumnRef("ts", ConnectorType.of("TIMESTAMPTZ", 27, 6));
        Map<Integer, ConnectorExpression> predicates = Map.of(1, new ConnectorComparison(
                ConnectorComparison.Operator.GE, ts,
                new ConnectorLiteral(ConnectorType.of("TIMESTAMPTZ", 27, 0),
                        LocalDateTime.parse("2021-11-07T00:00:00"))),
                2, new ConnectorComparison(ConnectorComparison.Operator.LT, ts,
                        new ConnectorLiteral(ts.getType(), LocalDateTime.parse("2021-11-08T00:00:00"))));
        for (String zone : List.of("UTC", "America/New_York", "Asia/Shanghai")) {
            Assertions.assertEquals(Set.of(1, 2), new IcebergPartitionFilterEvaluator(schema, spec,
                    partition, java.time.ZoneId.of(zone)).enforced(predicates));
        }
    }

    @Test
    void splitSerializationRetainsProofContextWithoutItsMemoizedState() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, 2);
        IcebergScanRange range = new IcebergScanRange.Builder().path("file:///table/data.parquet")
                .partitionFilterEvaluator(new IcebergPartitionFilterEvaluator(SCHEMA, spec, partition, ZoneOffset.UTC))
                .build();
        Map<Integer, ConnectorExpression> predicates = Map.of(4, intPredicate(ConnectorComparison.Operator.EQ, 2));
        Assertions.assertEquals(Set.of(4), range.getPartitionEnforcedConjuncts(predicates));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(range);
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            IcebergScanRange restored = (IcebergScanRange) input.readObject();
            Assertions.assertEquals(Set.of(4), restored.getPartitionEnforcedConjuncts(predicates));
        }
    }

    @Test
    void changedPartitionSourceTypeKeepsThePredicate() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, 2);
        Schema widened = new Schema(Types.NestedField.optional(2, "id", Types.LongType.get()));
        ConnectorType type = ConnectorType.of("BIGINT");
        ConnectorExpression predicate = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", type), new ConnectorLiteral(type, 2L));
        Assertions.assertTrue(new IcebergPartitionFilterEvaluator(widened, spec, partition, ZoneOffset.UTC)
                .enforced(Map.of(0, predicate)).isEmpty());
    }

    @Test
    void unrepresentableTimestampPartitionCannotHideColumnMaterialization() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, Math.toIntExact(LocalDate.of(12000, 1, 1).toEpochDay()));
        Map<Integer, ConnectorExpression> predicates = Map.of(
                0, timestampPredicate(ConnectorComparison.Operator.GE, "2000-01-01T00:00:00"),
                1, new ConnectorIsNull(new ConnectorColumnRef("ts", ConnectorType.of("DATETIMEV2", 27, 6)), true));
        Assertions.assertTrue(new IcebergPartitionFilterEvaluator(SCHEMA, spec, partition, ZoneOffset.UTC)
                .enforced(predicates).isEmpty());
    }
}
