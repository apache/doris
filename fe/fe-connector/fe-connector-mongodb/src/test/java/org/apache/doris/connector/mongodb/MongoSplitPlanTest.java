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

package org.apache.doris.connector.mongodb;

import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class MongoSplitPlanTest {
    @Test
    void integerBoundariesPreserveSignedWidthsAndUnboundedEnds() {
        var samples = List.<BsonValue>of(new BsonInt64(Long.MIN_VALUE), new BsonInt32(-1),
                new BsonInt64(9007199254740993L), new BsonInt64(Long.MAX_VALUE));
        var plan = MongoSplitPlan.ranges("sequence", "integer", samples, 4, 4);
        Assertions.assertEquals(4, plan.size());
        var first = plan.partition(0).getArray("$and");
        Assertions.assertEquals("int", first.get(0).asDocument().getDocument("$expr")
                .getArray("$in").get(1).asArray().get(0).asString().getValue());
        Assertions.assertFalse(first.get(1).asDocument().getDocument("sequence").containsKey("$gte"));
        Assertions.assertFalse(plan.partition(2).getArray("$and").get(1).asDocument()
                .getDocument("sequence").containsKey("$lt"));
        Assertions.assertTrue(plan.partition(3).getDocument("$expr").containsKey("$not"));
        for (long value : new long[] {Long.MIN_VALUE, -2, -1, 0, 9007199254740992L,
                9007199254740993L, 9007199254740994L, Long.MAX_VALUE}) {
            Assertions.assertEquals(1, matches(plan, new BsonInt64(value)), Long.toString(value));
        }
    }

    @Test
    void duplicateQuantilesKeepFrequencyButNeverPublishDuplicateBoundaries() {
        var samples = new ArrayList<BsonValue>();
        for (int i = 0; i < 100; i++) {
            samples.add(new BsonInt32(i < 80 ? 0 : i));
        }
        var plan = MongoSplitPlan.ranges("sequence", "integer", samples, 16, 8);
        Assertions.assertTrue(plan.size() < 8);
        for (long value = -1; value <= 101; value++) {
            Assertions.assertEquals(1, matches(plan, new BsonInt64(value)));
        }
        samples.clear();
        Assertions.assertEquals(1, matches(plan, new BsonInt32(81)));
    }

    @Test
    void dateMillisecondsAreIndependentOfSqlPrecisionAndTimezone() {
        var samples = List.<BsonValue>of(new BsonDateTime(Long.MIN_VALUE), new BsonDateTime(-1),
                new BsonDateTime(0), new BsonDateTime(1710064800123L), new BsonDateTime(Long.MAX_VALUE));
        var plan = MongoSplitPlan.ranges("observed", "date", samples, 5, 5);
        for (long value : new long[] {Long.MIN_VALUE, -2, -1, 0, 1, 1710064800122L,
                1710064800123L, 1710064800124L, Long.MAX_VALUE}) {
            Assertions.assertEquals(1, matches(plan, new BsonDateTime(value)));
        }
    }

    @Test
    void objectIdsUseAllUnsignedBytes() {
        var values = List.<BsonValue>of(new BsonObjectId(new ObjectId("000000000000000000000000")),
                new BsonObjectId(new ObjectId("7fffffffffffffffffffffff")),
                new BsonObjectId(new ObjectId("800000000000000000000000")),
                new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")));
        var plan = MongoSplitPlan.ranges("_id", "objectId", values, 4, 4);
        for (BsonValue value : values) {
            Assertions.assertEquals(1, matches(plan, value));
        }
    }

    @Test
    void complementTestsTheValueItselfInsteadOfArrayElements() {
        var plan = MongoSplitPlan.ranges("sequence", "integer", List.of(new BsonInt32(0),
                new BsonInt64(10), new BsonInt32(20)), 4, 4);
        var expression = plan.partition(0).getArray("$and").get(0).asDocument().getDocument("$expr");
        Assertions.assertEquals(new org.bson.BsonString("$sequence"),
                expression.getArray("$in").get(0).asDocument().get("$type"));
        // Missing, null, arrays (even with integer members), double and Decimal128 are NOT E.
        for (BsonValue value : List.of(org.bson.BsonNull.VALUE, new BsonArray(List.of(new BsonInt32(5))),
                new org.bson.BsonDouble(5), new org.bson.BsonDecimal128(new org.bson.types.Decimal128(
                        new java.math.BigDecimal("5.000000000000000000000000000000001"))))) {
            Assertions.assertEquals(1, matches(plan, value));
        }
    }

    @Test
    void randomizedDuplicateAndSkewedSamplesCoverTheWholeIntegerDomain() {
        var random = new Random(934351L);
        for (int trial = 0; trial < 200; trial++) {
            var values = new ArrayList<BsonValue>();
            for (int i = 0; i < 1 + random.nextInt(150); i++) {
                values.add(new BsonInt64(random.nextInt(7) == 0 ? random.nextLong() : random.nextInt(5)));
            }
            int admitted = 2 + random.nextInt(31);
            var plan = MongoSplitPlan.ranges("sequence", "integer", values, 32, admitted);
            Assertions.assertTrue(plan.size() <= admitted);
            for (int i = 0; i < 100; i++) {
                Assertions.assertEquals(1, matches(plan, new BsonInt64(random.nextLong())));
            }
        }
    }

    @Test
    void oneRequestedSplitAndEmptySamplesNeverMeanEmptyResults() {
        Assertions.assertEquals(new BsonDocument(), MongoSplitPlan.ranges("sequence", "integer",
                List.of(new BsonInt32(3)), 1, 1).partition(0));
        Assertions.assertEquals(new BsonDocument(), MongoSplitPlan.ranges("sequence", "integer",
                List.of(), 8, 8).partition(0));
    }

    @Test
    void partitionsAreOwnedAndUnsupportedFamiliesFail() {
        var plan = MongoSplitPlan.ranges("sequence", "integer",
                List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)), 3, 3);
        plan.partition(0).clear();
        Assertions.assertFalse(plan.partition(0).isEmpty());
        for (String family : List.of("decimal", "double", "string", "array", "timestamp")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> MongoSplitPlan.ranges("sequence", family, List.of(new BsonInt32(1)), 4, 4));
        }
    }

    @Test
    void idRangesUseTotalOrderExpressionsInsteadOfAComplement() {
        var samples = List.<BsonValue>of(new BsonInt64(Long.MIN_VALUE), new BsonInt64(-1),
                new BsonInt64(9007199254740993L), new BsonInt64(Long.MAX_VALUE));
        var plan = MongoSplitPlan.ranges("_id", "integer", samples, 4, 4);
        Assertions.assertEquals(4, plan.size());
        for (int i = 0; i < plan.size(); i++) {
            var expression = plan.partition(i).get("$expr");
            Assertions.assertNotNull(expression, "Use expression comparisons across BSON types");
            Assertions.assertFalse(expression.asDocument().containsKey("$not"));
            Assertions.assertFalse(plan.partition(i).toJson().contains("$type"));
        }
        Assertions.assertEquals(new BsonInt64(9007199254740993L), plan.partition(1)
                .getDocument("$expr").getArray("$and").get(1).asDocument().getArray("$lt").get(1));
    }

    @Test
    void oneIdBoundaryValueUsesACompleteSingleScan() {
        var plan = MongoSplitPlan.ranges("_id", "integer",
                List.of(new BsonInt32(7), new BsonInt64(7)), 16, 16);
        Assertions.assertEquals(1, plan.size());
        Assertions.assertEquals(new BsonDocument(), plan.partition(0));
    }

    @Test
    void idRangesPreserveDateMillisecondsAndSignedIntegerEndpoints() {
        for (String family : List.of("date", "integer")) {
            var values = new ArrayList<BsonValue>();
            for (long value : new long[] {Long.MIN_VALUE, -1, 0, 9007199254740993L, Long.MAX_VALUE}) {
                values.add(family.equals("date") ? new BsonDateTime(value) : new BsonInt64(value));
            }
            for (int admitted : new int[] {2, 3, 8, 32, 256}) {
                var plan = MongoSplitPlan.ranges("_id", family, values, 256, admitted);
                Assertions.assertTrue(plan.size() <= admitted);
                for (BsonValue value : values) {
                    Assertions.assertEquals(1, matches(plan, value));
                }
            }
        }
    }

    @Test
    void customFieldWithOnlyOneAdmittedRangeUsesACompleteSingleScan() {
        var samples = List.<BsonValue>of(new BsonInt64(Long.MIN_VALUE), new BsonInt64(-1),
                new BsonInt64(9007199254740993L), new BsonInt64(Long.MAX_VALUE));
        var plan = MongoSplitPlan.ranges("sequence", "integer", samples, 8, 2);
        Assertions.assertEquals(1, plan.size());
        Assertions.assertEquals("insufficient_boundaries", plan.reason());
        Assertions.assertEquals("", plan.family());
        Assertions.assertEquals(new BsonDocument(), plan.partition(0));
    }

    @Test
    void identicalCustomFieldSamplesNeverPublishTwoUnboundedTypeScans() {
        for (BsonValue value : List.of(new BsonInt64(9007199254740993L),
                new BsonDateTime(Long.MIN_VALUE), new BsonObjectId(new ObjectId("800000000000000000000001")))) {
            String family = value.isInt64() ? "integer" : value.isDateTime() ? "date" : "objectId";
            for (int admitted : new int[] {2, 3, 8, 256}) {
                var plan = MongoSplitPlan.ranges("sequence", family, List.of(value, value, value), 256, admitted);
                Assertions.assertEquals(1, plan.size(), family + ":" + admitted);
                Assertions.assertEquals("insufficient_boundaries", plan.reason());
                Assertions.assertEquals(new BsonDocument(), plan.partition(0));
            }
        }
    }

    @Test
    void encodedPartitionsAreExactAndOwnedByEachDescriptor() {
        for (String family : List.of("integer", "date", "objectId")) {
            List<BsonValue> values = switch (family) {
                case "integer" -> List.of(new BsonInt64(Long.MIN_VALUE), new BsonInt64(9007199254740993L),
                        new BsonInt64(Long.MAX_VALUE));
                case "date" -> List.of(new BsonDateTime(Long.MIN_VALUE), new BsonDateTime(-1),
                        new BsonDateTime(Long.MAX_VALUE));
                default -> List.of(new BsonObjectId(new ObjectId("000000000000000000000001")),
                        new BsonObjectId(new ObjectId("800000000000000000000001")),
                        new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")));
            };
            var plan = MongoSplitPlan.ranges("_id", family, values, 3, 3);
            var budgeted = plan.withDescriptorBudget(1024);
            for (int index = 0; index < plan.size(); index++) {
                byte[] expected = MongoSplitPlan.encode(plan.partition(index));
                byte[] exposed = budgeted.partitionBytes(index);
                Assertions.assertArrayEquals(expected, exposed);
                java.util.Arrays.fill(exposed, (byte) 0);
                Assertions.assertArrayEquals(expected, plan.partitionBytes(index));
                Assertions.assertArrayEquals(expected, budgeted.partitionBytes(index));
                Assertions.assertArrayEquals(expected, MongoSplitPlan.encode(plan.partition(index)));
            }
        }
    }

    private static int matches(MongoSplitPlan plan, BsonValue value) {
        boolean scalar = switch (plan.family()) {
            case "integer" -> value.isInt32() || value.isInt64();
            case "date" -> value.isDateTime();
            case "objectId" -> value.isObjectId();
            default -> false;
        };
        int matches = 0;
        for (int i = 0; i < plan.size(); i++) {
            BsonDocument partition = plan.partition(i);
            if ("_id".equals(plan.field()) && partition.containsKey("$expr")) {
                var expression = partition.getDocument("$expr");
                List<BsonValue> comparisons = expression.containsKey("$and")
                        ? expression.getArray("$and").getValues() : List.of(expression);
                boolean included = true;
                for (BsonValue comparison : comparisons) {
                    var predicate = comparison.asDocument();
                    boolean lower = predicate.containsKey("$gte");
                    var operands = predicate.getArray(lower ? "$gte" : "$lt");
                    int order = compare(value, operands.get(1));
                    included &= lower ? order >= 0 : order < 0;
                }
                if (included) {
                    matches++;
                }
            } else if (partition.isEmpty() || (partition.containsKey("$expr") && !scalar)) {
                matches++;
            } else if (partition.containsKey("$and") && scalar) {
                var interval = partition.getArray("$and").get(1).asDocument();
                var bounds = interval.getDocument(plan.field(), new BsonDocument());
                if ((!bounds.containsKey("$gte") || compare(value, bounds.get("$gte")) >= 0)
                        && (!bounds.containsKey("$lt") || compare(value, bounds.get("$lt")) < 0)) {
                    matches++;
                }
            }
        }
        return matches;
    }

    private static int compare(BsonValue left, BsonValue right) {
        if (left.isObjectId()) {
            return left.asObjectId().getValue().compareTo(right.asObjectId().getValue());
        }
        long a = left.isDateTime() ? left.asDateTime().getValue() : left.asNumber().longValue();
        long b = right.isDateTime() ? right.asDateTime().getValue() : right.asNumber().longValue();
        return Long.compare(a, b);
    }
}
