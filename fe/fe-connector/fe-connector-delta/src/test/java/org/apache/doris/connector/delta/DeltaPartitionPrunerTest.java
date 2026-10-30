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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeltaPartitionPrunerTest {

    private static final ConnectorType STRING = ConnectorType.of("STRING");
    private static final ConnectorType INT = ConnectorType.of("INT");

    @Test
    public void testEqualityAndInPruneOnlyMismatchingPartition() {
        Map<String, String> partitions = new LinkedHashMap<>();
        partitions.put("p", "one");
        ConnectorColumnRef column = new ConnectorColumnRef("p", STRING);

        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofString("one")),
                partitions));
        Assertions.assertFalse(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofString("two")),
                partitions));
        Assertions.assertTrue(mayMatch(new ConnectorIn(column,
                List.of(ConnectorLiteral.ofString("two"), ConnectorLiteral.ofString("one")), false),
                partitions));
        Assertions.assertFalse(mayMatch(new ConnectorIn(column,
                List.of(ConnectorLiteral.ofString("two"), ConnectorLiteral.ofString("three")), false),
                partitions));
    }

    @Test
    public void testNullPartitionAndLogicalExpressions() {
        Map<String, String> partitions = new LinkedHashMap<>();
        partitions.put("p", null);
        ConnectorColumnRef column = new ConnectorColumnRef("p", STRING);

        Assertions.assertTrue(mayMatch(new ConnectorIsNull(column, false), partitions));
        Assertions.assertFalse(mayMatch(new ConnectorIsNull(column, true), partitions));
        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, column,
                ConnectorLiteral.ofNull(STRING)), partitions));
        Assertions.assertFalse(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofString("one")),
                partitions));

        ConnectorExpression mismatching = new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofString("one"));
        ConnectorExpression matching = new ConnectorIsNull(column, false);
        Assertions.assertFalse(mayMatch(new ConnectorAnd(List.of(mismatching, matching)), partitions));
        Assertions.assertTrue(mayMatch(new ConnectorOr(List.of(mismatching, matching)), partitions));
    }

    @Test
    public void testNumericEqualityUsesDeltaCanonicalValue() {
        Map<String, String> partitions = Map.of("p", "001");
        ConnectorColumnRef column = new ConnectorColumnRef("p", INT);
        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofInt(1)), partitions));
        Assertions.assertFalse(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofInt(2)), partitions));
    }

    @Test
    public void testDateTimeAndUnknownBooleanValuesAreConservative() {
        Map<String, String> partitions = new LinkedHashMap<>();
        partitions.put("day", "2024-01-02");
        partitions.put("ts", "2024-01-02 03:04:05");
        partitions.put("flag", "unexpected");

        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("day", ConnectorType.of("DATEV2")),
                ConnectorLiteral.ofDate(LocalDate.of(2024, 1, 2))), partitions));
        Assertions.assertFalse(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("day", ConnectorType.of("DATEV2")),
                ConnectorLiteral.ofDate(LocalDate.of(2024, 1, 3))), partitions));
        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", ConnectorType.of("DATETIMEV2")),
                ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 1, 2, 3, 4, 5))), partitions));
        Assertions.assertFalse(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", ConnectorType.of("DATETIMEV2")),
                ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 1, 2, 3, 4, 6))), partitions));
        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true)), partitions));
    }

    @Test
    public void testUnknownExpressionsAndNonPartitionColumnsAreNeverPruned() {
        Map<String, String> partitions = Map.of("p", "one");
        ConnectorColumnRef nonPartition = new ConnectorColumnRef("id", INT);
        Assertions.assertTrue(mayMatch(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, nonPartition, ConnectorLiteral.ofInt(1)),
                partitions));
        Assertions.assertTrue(mayMatch(new ConnectorNot(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, nonPartition, ConnectorLiteral.ofInt(1))),
                partitions));
    }

    private static boolean mayMatch(ConnectorExpression expression, Map<String, String> partitions) {
        return DeltaPartitionPruner.mayMatch(partitions, expression);
    }
}
