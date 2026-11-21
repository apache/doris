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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorBetween;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

class MongoPredicateTranslatorTest {
    private MongoSchema schema(String name, List<String> path, String type, List<String> tags) {
        Document column = new Document("name", name).append("path", path).append("doris_type", type)
                .append("bson_types", tags);
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.schema", new Document("sample.records", new Document("columns", List.of(column))).toJson()));
        return MongoSchema.resolve("sample.records", new Document("type", "collection"), properties);
    }

    private MongoSchema schema(String type, String... tags) {
        return schema("v", List.of("Source"), type, List.of(tags));
    }

    private ConnectorColumnRef column(MongoSchema schema) {
        return new ConnectorColumnRef("v", schema.getBindings().get(0).column().getType());
    }

    private ConnectorExpression comparison(MongoSchema schema, ConnectorComparison.Operator op,
            ConnectorLiteral value) {
        return new ConnectorComparison(op, column(schema), value);
    }

    private MongoPredicateTranslator.Result translate(MongoSchema schema, ConnectorExpression expression) {
        return new MongoPredicateTranslator(schema, true).translate(expression);
    }

    @Test
    void integerComparisonPreservesLongAboveDoublePrecisionAndSourceCase() {
        var schema = schema("BIGINT", "long", "null");
        long value = 9007199254740993L;
        var result = translate(schema, comparison(schema, ConnectorComparison.Operator.GE, ConnectorLiteral.ofLong(value)));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET, result.kind());
        Assertions.assertEquals(value, result.filter().get("Source", Document.class).get("$gte"));
    }

    @Test
    void reversedComparisonReversesTheRange() {
        var schema = schema("BIGINT", "long");
        var expression = new ConnectorComparison(ConnectorComparison.Operator.LT,
                ConnectorLiteral.ofLong(7), column(schema));
        Assertions.assertEquals(new Document("Source", new Document("$gt", 7L)),
                translate(schema, expression).filter());
    }

    @Test
    void dateRangeUsesBsonDateInUtcAtNegativeEpoch() {
        var schema = schema("DATETIME(3)", "date");
        var instant = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999000000);
        var result = translate(schema, comparison(schema, ConnectorComparison.Operator.GE,
                ConnectorLiteral.ofDatetime(instant)));
        Assertions.assertEquals(new Date(-1), result.filter().get("Source", Document.class).get("$gte"));
        Assertions.assertEquals(-1, instant.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    void dateConstantsCannotLoseSubMillisecondPrecision() {
        var schema = schema("DATETIME(6)", "date");
        var value = ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1000));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                translate(schema, comparison(schema, ConnectorComparison.Operator.EQ, value)).kind());
    }

    @Test
    void explicitEpochBindingUsesDateInsteadOfAnIntegerQuery() {
        var schema = schema("BIGINT", "date");
        var result = translate(schema, comparison(schema, ConnectorComparison.Operator.LT, ConnectorLiteral.ofLong(1)));
        Assertions.assertEquals(new Date(1), result.filter().get("Source", Document.class).get("$lt"));
    }

    @Test
    void decimalLiteralRemainsDecimal128WithoutDoubleConversion() {
        var schema = schema("DECIMAL(38,10)", "decimal");
        var value = new BigDecimal("9007199254740993.0000000001");
        var result = translate(schema, comparison(schema, ConnectorComparison.Operator.EQ,
                ConnectorLiteral.ofDecimal(value, 38, 10)));
        Assertions.assertEquals(new Decimal128(value), result.filter().get("Source", Document.class).get("$eq"));
    }

    @Test
    void allDatetimeScalesRetainUtcMillisecondsAndRejectSubMilliseconds() {
        for (int scale = 0; scale <= 6; scale++) {
            var schema = schema("DATETIME(" + scale + ")", "date");
            for (long millis : new long[] {-1001, -1000, -1, 0, 1, 1000, 253402300799999L}) {
                var literal = ConnectorLiteral.ofDatetime(LocalDateTime.ofInstant(
                        new Date(millis).toInstant(), ZoneOffset.UTC));
                var result = translate(schema, comparison(schema, ConnectorComparison.Operator.GE, literal));
                Assertions.assertEquals(new Date(millis), result.filter().get("Source", Document.class).get("$gte"));
            }
            for (int nanos : new int[] {1000, 999000, 999999000}) {
                var literal = ConnectorLiteral.ofDatetime(LocalDateTime.of(1969, 12, 31, 23, 59, 59, nanos));
                Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                        translate(schema, comparison(schema, ConnectorComparison.Operator.LT, literal)).kind());
            }
        }
    }

    @Test
    void decimalPrecisionScaleCandidatesRemainExactOrStayLocal() {
        for (int precision : new int[] {1, 9, 10, 18, 19, 38}) {
            for (int scale : new int[] {0, precision / 2, precision}) {
                var schema = schema("DECIMAL(" + precision + "," + scale + ")", "decimal");
                var exact = BigDecimal.ONE.scaleByPowerOfTen(-scale);
                var literal = ConnectorLiteral.ofDecimal(exact, precision, scale);
                var result = translate(schema, comparison(schema, ConnectorComparison.Operator.EQ, literal));
                Assertions.assertEquals(new Decimal128(exact), result.filter().get("Source", Document.class).get("$eq"));
            }
        }
        var schema = schema("DECIMAL(38,0)", "decimal");
        for (String value : List.of("99999999999999999999999999999999999999", "1E+6145", "1E-6177")) {
            var literal = ConnectorLiteral.ofDecimal(new BigDecimal(value), 38, 0);
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                    translate(schema, comparison(schema, ConnectorComparison.Operator.EQ, literal)).kind());
        }
    }

    @Test
    void inDropsNullArmsButNeverRewritesNotIn() {
        var schema = schema("INT", "int");
        var values = List.<ConnectorExpression>of(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofNull(ConnectorType.of("INT")));
        Assertions.assertEquals(new Document("Source", new Document("$in", List.of(1L))),
                translate(schema, new ConnectorIn(column(schema), values, false)).filter());
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                translate(schema, new ConnectorIn(column(schema), values, true)).kind());
    }

    @Test
    void betweenKeepsBothBoundsWithoutOverwritingTheSameField() {
        var schema = schema("INT", "int");
        var result = translate(schema, new ConnectorBetween(column(schema),
                ConnectorLiteral.ofLong(1), ConnectorLiteral.ofLong(3)));
        Assertions.assertEquals(2, result.filter().getList("$and", Document.class).size());
    }

    @Test
    void andCanUseOneCandidateButOrRequiresEveryArm() {
        var schema = schema("BIGINT", "long");
        var supported = comparison(schema, ConnectorComparison.Operator.EQ, ConnectorLiteral.ofLong(1));
        var unsupported = new ConnectorNot(supported);
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET,
                translate(schema, new ConnectorAnd(List.of(supported, unsupported))).kind());
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                translate(schema, new ConnectorOr(List.of(supported, unsupported))).kind());
        Assertions.assertEquals(2, translate(schema, new ConnectorOr(List.of(supported, supported)))
                .filter().getList("$or", Document.class).size());
    }

    @Test
    void nullOnMismatchMakesIsNullUnsafeButIsNotNullHasAValidSuperset() {
        var schema = schema("TINYINT", "int");
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                translate(schema, new ConnectorIsNull(column(schema), false)).kind());
        Assertions.assertEquals(new Document("Source", new Document("$ne", null)),
                translate(schema, new ConnectorIsNull(column(schema), true)).filter());
    }

    @Test
    void nullSafeEqualityDoesNotConfuseNonNullWithIsNull() {
        var schema = schema("INT", "int");
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET, translate(schema,
                comparison(schema, ConnectorComparison.Operator.EQ_FOR_NULL, ConnectorLiteral.ofLong(3))).kind());
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                comparison(schema, ConnectorComparison.Operator.EQ_FOR_NULL,
                        ConnectorLiteral.ofNull(ConnectorType.of("INT")))).kind());
    }

    @Test
    void functionsFloatAndImplicitStringNumericCoercionStayLocal() {
        for (String type : List.of("DOUBLE", "STRING")) {
            var schema = schema(type);
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                    translate(schema, comparison(schema, ConnectorComparison.Operator.EQ,
                            ConnectorLiteral.ofLong(1))).kind());
        }
        var schema = schema("BIGINT");
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                comparison(schema, ConnectorComparison.Operator.EQ, ConnectorLiteral.ofLong(1))).kind());
    }

    @Test
    void literalDotDollarAndNestedPathsStayLocal() {
        for (List<String> path : List.of(List.of("a.b"), List.of("$x"), List.of("a", "b"))) {
            var schema = schema("v", path, "BIGINT", List.of("long"));
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                    comparison(schema, ConnectorComparison.Operator.EQ, ConnectorLiteral.ofLong(1))).kind());
        }
    }

    @Test
    void likeUsesOnlyAnEscapedNecessaryLiteralAndKeepsResidualClassification() {
        var schema = schema("STRING", "string");
        for (Map.Entry<String, String> example : Map.of("%a.b%", "a\\.b", "x_y%abc", "abc",
                "%中文%", "中文", "%a\\%%", "a%", "%[x]%", "\\[x\\]").entrySet()) {
            var predicate = new ConnectorLike(ConnectorLike.Operator.LIKE, column(schema),
                    ConnectorLiteral.ofString(example.getKey()));
            var result = translate(schema, predicate);
            Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET, result.kind());
            Assertions.assertEquals(example.getValue(), result.filter().get("Source", Document.class).get("$regex"));
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                    new MongoPredicateTranslator(schema, false).translate(predicate).kind());
        }
    }

    @Test
    void regexWildcardOnlyDanglingEscapeAndNulAreNotTranslated() {
        var schema = schema("STRING", "string");
        for (String pattern : List.of("%_", "", "abc\\", "a\u0000b", "\\q")) {
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                    new ConnectorLike(ConnectorLike.Operator.LIKE, column(schema), ConnectorLiteral.ofString(pattern))).kind());
        }
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                new ConnectorLike(ConnectorLike.Operator.REGEXP, column(schema), ConnectorLiteral.ofString("a"))).kind());
    }

    @Test
    void predicateBudgetsBoundInListsAndRecursiveTrees() {
        var schema = schema("INT", "int");
        List<ConnectorExpression> list = new ArrayList<>();
        for (int i = 0; i < 1025; i++) {
            list.add(ConnectorLiteral.ofLong(i));
        }
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED,
                translate(schema, new ConnectorIn(column(schema), list, false)).kind());
        ConnectorExpression expression = comparison(schema, ConnectorComparison.Operator.EQ, ConnectorLiteral.ofLong(1));
        for (int i = 0; i < 65; i++) {
            expression = new ConnectorAnd(List.of(expression));
        }
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema, expression).kind());
    }

    @Test
    void oversizedTextLiteralDoesNotAllocateAnUnboundedBsonCommand() {
        var schema = schema("STRING", "string");
        Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema,
                comparison(schema, ConnectorComparison.Operator.EQ,
                        ConnectorLiteral.ofString("x".repeat(65537)))).kind());
    }

    @Test
    void expressionSpiDatetimeScaleMatchesTheSchemaDatetimePrecision() {
        var schema = schema("DATETIME(3)", "date");
        var ref = new ConnectorColumnRef("v", ConnectorType.of("DATETIMEV2", -1, 3));
        var filter = new ConnectorComparison(ConnectorComparison.Operator.GE, ref,
                ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 1, 1, 0, 0)));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET, translate(schema, filter).kind());
    }

    @Test
    void expressionSpiDecimalStorageNameMatchesTheSchemaDecimalFamily() {
        var schema = schema("DECIMAL(20,2)", "decimal");
        var ref = new ConnectorColumnRef("v", ConnectorType.of("DECIMAL128", 20, 2));
        var filter = new ConnectorComparison(ConnectorComparison.Operator.EQ, ref,
                ConnectorLiteral.ofDecimal(new BigDecimal("1.20"), 20, 2));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET, translate(schema, filter).kind());
    }

    @Test
    void arrayAndJsonIsNotNullCannotUseMongoArrayElementNullSemantics() {
        for (String type : List.of("ARRAY<STRING>", "JSON")) {
            var schema = schema(type, "array");
            var predicate = new ConnectorIsNull(column(schema), true);
            Assertions.assertEquals(MongoPredicateTranslator.Kind.UNSUPPORTED, translate(schema, predicate).kind());
        }
    }

}
