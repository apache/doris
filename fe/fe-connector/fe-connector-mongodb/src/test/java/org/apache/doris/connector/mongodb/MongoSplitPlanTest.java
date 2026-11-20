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
    void everyCustomPartitionUsesTheCompleteBsonTypeList() {
        for (String family : List.of("integer", "date", "objectId")) {
            var samples = new ArrayList<BsonValue>();
            for (int i = 0; i < 8; i++) {
                BsonValue sample = switch (family) {
                    case "integer" -> new BsonInt64(i);
                    case "date" -> new BsonDateTime(i);
                    default -> new BsonObjectId(new ObjectId(String.format("%024x", i)));
                };
                samples.add(sample);
            }
            var plan = MongoSplitPlan.ranges("key", family, samples, 4, 4);
            var expected = "integer".equals(family) ? List.of("int", "long") : List.of(family);
            for (int i = 0; i < plan.size(); i++) {
                var partition = plan.partition(i);
                var eligibility = partition.containsKey("$and")
                        ? partition.getArray("$and").get(0).asDocument().getDocument("$expr")
                        : partition.getDocument("$expr").getArray("$not").get(0).asDocument();
                Assertions.assertEquals(new BsonArray(expected.stream().map(org.bson.BsonString::new).toList()),
                        eligibility.getArray("$in").get(1));
                Assertions.assertEquals(new org.bson.BsonString("$key"), eligibility.getArray("$in").get(0)
                        .asDocument().get("$type"));
            }
        }
    }

    @Test
    void oracleReadsEligibilityFromBsonAndDetectsMutatedTypeLists() {
        var plan = MongoSplitPlan.ranges("key", "integer",
                List.of(new BsonInt32(0), new BsonInt64(10), new BsonInt32(20)), 4, 4);
        var partition = plan.partition(0);
        var value = new BsonInt64(1);
        Assertions.assertTrue(matchesPartition(partition, "key", value));
        partition.getArray("$and").get(0).asDocument().getDocument("$expr").getArray("$in")
                .set(1, new BsonArray(List.of(new org.bson.BsonString("int"))));
        Assertions.assertFalse(matchesPartition(partition, "key", value));
        var complement = plan.partition(plan.size() - 1);
        Assertions.assertFalse(matchesPartition(complement, "key", value));
        complement.getDocument("$expr").getArray("$not").get(0).asDocument().getArray("$in")
                .set(1, new BsonArray(List.of(new org.bson.BsonString("int"))));
        Assertions.assertTrue(matchesPartition(complement, "key", value));
    }

    @Test
    void idRangesIncludeUnsampledBsonTypesInTheKnownBsonOrder() {
        // _id forbids arrays, regex and undefined; every other BSON type is represented here.
        var ordered = List.<BsonValue>of(new org.bson.BsonMinKey(), org.bson.BsonNull.VALUE,
                new org.bson.BsonDouble(Double.NaN), new org.bson.BsonDouble(Double.NEGATIVE_INFINITY),
                new BsonInt64(Long.MIN_VALUE), new BsonInt64(-1), new BsonInt32(0),
                new org.bson.BsonDecimal128(org.bson.types.Decimal128.parse("0.25")), new org.bson.BsonDouble(0.5),
                new BsonInt64(1), new org.bson.BsonDouble(9007199254740992d), new BsonInt64(9007199254740993L),
                new org.bson.BsonDecimal128(org.bson.types.Decimal128.parse("9007199254740993.25")),
                new BsonInt64(Long.MAX_VALUE), new org.bson.BsonDouble(Double.POSITIVE_INFINITY),
                new org.bson.BsonSymbol("alpha"), new org.bson.BsonString("key"),
                new BsonDocument("key", new BsonInt32(1)), new org.bson.BsonBinary(new byte[] {1}),
                new BsonObjectId(new ObjectId("000000000000000000000001")),
                new BsonObjectId(new ObjectId("800000000000000000000001")),
                new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")),
                org.bson.BsonBoolean.FALSE, org.bson.BsonBoolean.TRUE, new BsonDateTime(Long.MIN_VALUE),
                new BsonDateTime(-1), new BsonDateTime(0), new BsonDateTime(1), new BsonDateTime(Long.MAX_VALUE),
                new org.bson.BsonTimestamp(1, 1),
                new org.bson.BsonDbPointer("sample.records", new ObjectId("000000000000000000000001")),
                new org.bson.BsonJavaScript("return 1"),
                new org.bson.BsonJavaScriptWithScope("return key", new BsonDocument("key", new BsonInt32(1))),
                new org.bson.BsonMaxKey());
        for (String family : List.of("integer", "date", "objectId")) {
            List<BsonValue> samples = switch (family) {
                case "integer" -> List.of(new BsonInt64(-1), new BsonInt32(0), new BsonInt64(1));
                case "date" -> List.of(new BsonDateTime(-1), new BsonDateTime(0), new BsonDateTime(1));
                default -> List.of(new BsonObjectId(new ObjectId("000000000000000000000001")),
                        new BsonObjectId(new ObjectId("800000000000000000000001")),
                        new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")));
            };
            var plan = MongoSplitPlan.ranges("_id", family, samples, 3, 3);
            int firstBoundary = ordered.indexOf(samples.get(1));
            int secondBoundary = ordered.indexOf(samples.get(2));
            for (int index = 0; index < ordered.size(); index++) {
                var value = ordered.get(index);
                int expected = (index >= firstBoundary ? 1 : 0) + (index >= secondBoundary ? 1 : 0);
                Assertions.assertEquals(1, matches(plan, value), family + ":" + value);
                Assertions.assertTrue(matchesPartition(plan.partition(expected), "_id", value), family + ":" + value);
            }
        }
    }

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
    void crossNumericIdComparisonsKeepTheExactBinaryDoubleNearLongBounds() {
        var value = new org.bson.BsonDouble(1000000000000000128d);
        for (long bound : new long[] {1000000000000000110L, 1000000000000000129L}) {
            var plan = MongoSplitPlan.ranges("_id", "integer", List.of(new BsonInt64(Long.MIN_VALUE),
                    new BsonInt64(bound), new BsonInt64(Long.MAX_VALUE)), 3, 3);
            int expected = bound < 1000000000000000128L ? 1 : 0;
            Assertions.assertEquals(1, matches(plan, value));
            Assertions.assertTrue(matchesPartition(plan.partition(expected), "_id", value));
            Assertions.assertTrue(matchesPartition(plan.partition(0), "_id",
                    new org.bson.BsonDouble(Math.nextDown(value.getValue()))));
            Assertions.assertTrue(matchesPartition(plan.partition(1), "_id",
                    new org.bson.BsonDouble(Math.nextUp(value.getValue()))));
            Assertions.assertTrue(matchesPartition(plan.partition(1), "_id", new org.bson.BsonDecimal128(
                    org.bson.types.Decimal128.parse("1000000000000000129.000000000001"))));
            Assertions.assertTrue(matchesPartition(plan.partition(2), "_id", new org.bson.BsonDouble(0x1.0p63)));
            Assertions.assertTrue(matchesPartition(plan.partition(1), "_id",
                    new org.bson.BsonDouble(Math.nextDown(0x1.0p63))));
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
        Assertions.assertEquals(1, matches(plan, null));
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
            for (int index = 0; index < plan.size(); index++) {
                byte[] expected = MongoSplitPlan.encode(plan.partition(index));
                byte[] exposed = plan.partitionBytes(index);
                Assertions.assertArrayEquals(expected, exposed);
                java.util.Arrays.fill(exposed, (byte) 0);
                Assertions.assertArrayEquals(expected, plan.partitionBytes(index));
                Assertions.assertArrayEquals(expected, MongoSplitPlan.encode(plan.partition(index)));
            }
        }
    }

    private static int matches(MongoSplitPlan plan, BsonValue value) {
        int count = 0;
        for (int i = 0; i < plan.size(); i++) {
            if (matchesPartition(plan.partition(i), plan.field(), value)) {
                count++;
            }
        }
        return count;
    }

    private static boolean matchesPartition(BsonDocument partition, String field, BsonValue value) {
        if (partition.isEmpty()) {
            return true;
        }
        if (partition.containsKey("$expr")) {
            return expression(partition.getDocument("$expr"), field, value);
        }
        var conjunction = partition.getArray("$and");
        if (!expression(conjunction.get(0).asDocument().getDocument("$expr"), field, value)) {
            return false;
        }
        var bounds = conjunction.get(1).asDocument().getDocument(field, new BsonDocument());
        return (!bounds.containsKey("$gte") || compare(value, bounds.get("$gte")) >= 0)
                && (!bounds.containsKey("$lt") || compare(value, bounds.get("$lt")) < 0);
    }

    private static boolean expression(BsonDocument expression, String field, BsonValue value) {
        if (expression.containsKey("$and")) {
            return expression.getArray("$and").stream().allMatch(v -> expression(v.asDocument(), field, value));
        }
        if (expression.containsKey("$not")) {
            return !expression(expression.getArray("$not").get(0).asDocument(), field, value);
        }
        if (expression.containsKey("$in")) {
            var operands = expression.getArray("$in");
            Assertions.assertEquals("$" + field, operands.get(0).asDocument().getString("$type").getValue());
            return operands.get(1).asArray().contains(new org.bson.BsonString(typeName(value)));
        }
        boolean lower = expression.containsKey("$gte");
        var operands = expression.getArray(lower ? "$gte" : "$lt");
        Assertions.assertEquals("$" + field, operands.get(0).asString().getValue());
        int order = compare(value, operands.get(1));
        return lower ? order >= 0 : order < 0;
    }

    private static String typeName(BsonValue value) {
        if (value == null) {
            return "missing";
        }
        return switch (value.getBsonType()) {
            case INT32 -> "int";
            case INT64 -> "long";
            case DECIMAL128 -> "decimal";
            case DATE_TIME -> "date";
            case OBJECT_ID -> "objectId";
            case BINARY -> "binData";
            case BOOLEAN -> "bool";
            case REGULAR_EXPRESSION -> "regex";
            case JAVASCRIPT_WITH_SCOPE -> "javascriptWithScope";
            case DB_POINTER -> "dbPointer";
            case MIN_KEY -> "minKey";
            case MAX_KEY -> "maxKey";
            default -> value.getBsonType().name().toLowerCase(java.util.Locale.ROOT);
        };
    }

    private static int typeOrder(BsonValue value) {
        // Independent reference: MongoDB's BSON comparison order, not the sampled Java family.
        // See https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/.
        return switch (value.getBsonType()) {
            case MIN_KEY -> 0;
            case NULL -> 1;
            case DOUBLE, INT32, INT64, DECIMAL128 -> 2;
            case STRING, SYMBOL -> 3;
            case DOCUMENT -> 4;
            case ARRAY -> 5;
            case BINARY -> 6;
            case OBJECT_ID -> 7;
            case BOOLEAN -> 8;
            case DATE_TIME -> 9;
            case TIMESTAMP -> 10;
            case REGULAR_EXPRESSION -> 11;
            case DB_POINTER -> 12;
            case JAVASCRIPT -> 13;
            case JAVASCRIPT_WITH_SCOPE -> 14;
            case MAX_KEY -> 15;
            default -> throw new AssertionError("Unsupported reference BSON type: " + value.getBsonType());
        };
    }

    private static int numberOrder(BsonValue value) {
        if (value.isDouble()) {
            double number = value.asDouble().getValue();
            return Double.isNaN(number) ? -2 : number == Double.NEGATIVE_INFINITY ? -1
                    : number == Double.POSITIVE_INFINITY ? 1 : 0;
        }
        if (value.isDecimal128()) {
            var number = value.asDecimal128().getValue();
            return number.isNaN() ? -2 : number.isInfinite() ? (number.isNegative() ? -1 : 1) : 0;
        }
        return 0;
    }

    private static java.math.BigDecimal finiteNumber(BsonValue value) {
        if (value.isDecimal128()) {
            return value.asDecimal128().getValue().bigDecimalValue();
        }
        // Round-trip text can turn the exact double 1e18+128 into decimal 1e18+130,
        // crossing an int64 bound at 1e18+129. Compare the exact binary value instead.
        return value.isDouble() ? new java.math.BigDecimal(value.asDouble().getValue())
                : java.math.BigDecimal.valueOf(value.asNumber().longValue());
    }

    private static int compare(BsonValue left, BsonValue right) {
        int type = Integer.compare(typeOrder(left), typeOrder(right));
        if (type != 0) {
            return type;
        }
        if (left.isNumber()) {
            int order = Integer.compare(numberOrder(left), numberOrder(right));
            return order != 0 || numberOrder(left) != 0 ? order : finiteNumber(left).compareTo(finiteNumber(right));
        }
        if (left.isObjectId()) {
            return left.asObjectId().getValue().compareTo(right.asObjectId().getValue());
        }
        if (left.isDateTime()) {
            return Long.compare(left.asDateTime().getValue(), right.asDateTime().getValue());
        }
        throw new AssertionError("Reference bounds must be integer, Date or ObjectId");
    }
}
