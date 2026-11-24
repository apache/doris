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
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.bson.Document;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Candidate filters only: the scan provider never consumes the original Doris predicates. */
final class MongoPredicateTranslator {
    enum Kind { EXACT, SUPERSET, UNSUPPORTED }

    record Result(Kind kind, Document filter) {
        static Result unsupported() {
            return new Result(Kind.UNSUPPORTED, new Document());
        }
    }

    private static final int MAX_TERMS = 1024;
    private static final int MAX_DEPTH = 64;
    private final Map<String, MongoSchema.Binding> bindings;
    private final boolean like;
    private int terms;

    MongoPredicateTranslator(MongoSchema schema, boolean like) {
        bindings = schema.getBindings().stream().collect(Collectors.toMap(MongoSchema.Binding::getName, b -> b));
        this.like = like;
    }

    Result translate(ConnectorExpression expression) {
        terms = 0;
        return translate(expression, 0);
    }

    private Result translate(ConnectorExpression expression, int depth) {
        if (depth > MAX_DEPTH || ++terms > MAX_TERMS) {
            return Result.unsupported();
        }
        if (expression instanceof ConnectorAnd || expression instanceof ConnectorOr) {
            boolean and = expression instanceof ConnectorAnd;
            List<Document> filters = new ArrayList<>();
            boolean exact = true;
            for (ConnectorExpression child : expression.getChildren()) {
                Result result = translate(child, depth + 1);
                if (result.kind() == Kind.UNSUPPORTED) {
                    if (!and) {
                        // Dropping even one OR arm loses rows; AND can keep independently safe candidates.
                        return Result.unsupported();
                    }
                    exact = false;
                } else {
                    filters.add(result.filter());
                    exact &= result.kind() == Kind.EXACT;
                }
            }
            return filters.isEmpty() ? Result.unsupported()
                    : new Result(exact ? Kind.EXACT : Kind.SUPERSET,
                            new Document(and ? "$and" : "$or", filters));
        }
        if (expression instanceof ConnectorComparison comparison) {
            ConnectorExpression column = comparison.getLeft();
            ConnectorExpression constant = comparison.getRight();
            String operator = switch (comparison.getOperator()) {
                case EQ, EQ_FOR_NULL -> "$eq";
                case GT -> "$gt";
                case GE -> "$gte";
                case LT -> "$lt";
                case LE -> "$lte";
                default -> null;
            };
            if (operator == null) {
                return Result.unsupported();
            }
            if (column instanceof ConnectorLiteral && constant instanceof ConnectorColumnRef) {
                ConnectorExpression tmp = column;
                column = constant;
                constant = tmp;
                operator = switch (operator) {
                    case "$gt" -> "$lt";
                    case "$gte" -> "$lte";
                    case "$lt" -> "$gt";
                    case "$lte" -> "$gte";
                    default -> operator;
                };
            }
            MongoSchema.Binding binding = binding(column);
            if (binding == null || !(constant instanceof ConnectorLiteral literal)) {
                return Result.unsupported();
            }
            Object value = typedValue(binding, literal);
            return value == null ? Result.unsupported() : candidate(binding, operator, value);
        }
        if (expression instanceof ConnectorBetween between) {
            return translate(new ConnectorAnd(List.of(
                    new ConnectorComparison(ConnectorComparison.Operator.GE, between.getValue(), between.getLower()),
                    new ConnectorComparison(ConnectorComparison.Operator.LE, between.getValue(), between.getUpper()))),
                    depth + 1);
        }
        if (expression instanceof ConnectorIn in) {
            MongoSchema.Binding binding = binding(in.getValue());
            if (in.isNegated() || binding == null || in.getInList().isEmpty()
                    || in.getInList().size() > MAX_TERMS) {
                return Result.unsupported();
            }
            List<Object> values = new ArrayList<>();
            for (ConnectorExpression item : in.getInList()) {
                if (!(item instanceof ConnectorLiteral literal)) {
                    return Result.unsupported();
                }
                // NULL contributes no TRUE arm to a positive IN; NOT IN deliberately stays local.
                if (literal.isNull()) {
                    continue;
                }
                Object value = typedValue(binding, literal);
                if (value == null) {
                    return Result.unsupported();
                }
                values.add(value);
            }
            return candidate(binding, "$in", values);
        }
        if (expression instanceof ConnectorIsNull isNull && isNull.isNegated()) {
            MongoSchema.Binding binding = binding(isNull.getOperand());
            if (binding == null || !Set.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT",
                    "FLOAT", "DOUBLE", "STRING", "VARBINARY", "DATETIMEV2", "DECIMALV3")
                    .contains(binding.column().getType().getTypeName())) {
                return Result.unsupported();
            }
            // MongoDB's $ne:null rejects arrays containing null. ARRAY[NULL] (or JSON [null])
            // is a non-NULL SQL value, so complex targets must keep this predicate entirely local.
            return candidate(binding, "$ne", null);
        }
        if (expression instanceof ConnectorLike predicate && like
                && predicate.getOperator() == ConnectorLike.Operator.LIKE
                && predicate.getPattern() instanceof ConnectorLiteral literal
                && literal.getValue() instanceof String pattern) {
            MongoSchema.Binding binding = binding(predicate.getValue());
            if (binding == null || !"STRING".equals(binding.column().getType().getTypeName())) {
                return Result.unsupported();
            }
            String regex = requiredLikeLiteral(pattern);
            return regex == null ? Result.unsupported() : candidate(binding, "$regex", regex);
        }
        return Result.unsupported();
    }

    private MongoSchema.Binding binding(ConnectorExpression expression) {
        if (!(expression instanceof ConnectorColumnRef ref)) {
            return null;
        }
        MongoSchema.Binding binding = bindings.get(ref.getColumnName());
        // A dotted MongoDB path traverses arrays implicitly, unlike the decoder. Start with scalar roots.
        return binding != null && binding.getPath().size() == 1 && safePath(binding.getPath().get(0))
                && matchesReferenceType(binding.column().getType(), ref.getType()) ? binding : null;
    }

    private static boolean matchesReferenceType(ConnectorType schema, ConnectorType expression) {
        if (schema.equals(expression)) {
            return true;
        }
        // The schema SPI stores DATETIME scale in precision; the expression SPI uses scale.
        // For example, DATETIME(3) arrives as DATETIMEV2(-1,3), not the schema's (3,0).
        if ("DATETIMEV2".equals(schema.getTypeName()) && "DATETIMEV2".equals(expression.getTypeName())) {
            return schema.getPrecision() == Math.max(0, expression.getScale());
        }
        // Decimal expressions name their physical width after schema conversion (e.g. DECIMAL128).
        return "DECIMALV3".equals(schema.getTypeName())
                && Set.of("DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256").contains(expression.getTypeName())
                && schema.getPrecision() == expression.getPrecision() && schema.getScale() == expression.getScale();
    }

    static boolean safePath(String segment) {
        return !segment.isEmpty() && !segment.contains(".") && !segment.contains("$") && segment.indexOf(0) < 0;
    }

    private static Result candidate(MongoSchema.Binding binding, String operator, Object value) {
        // MongoDB may also match arrays or other numeric tags. NULL-on-mismatch and the residual
        // discard those extra rows; no scalar guarantee is inferred from a validator on historical data.
        return new Result(Kind.SUPERSET, new Document(binding.getPath().get(0), new Document(operator, value)));
    }

    private static Object typedValue(MongoSchema.Binding binding, ConnectorLiteral literal) {
        if (literal.isNull()) {
            return null;
        }
        Document mapping = binding.toDocument();
        List<Integer> tags = mapping.getList("bson_types", Integer.class).stream().filter(t -> t != 10).toList();
        String type = binding.column().getType().getTypeName();
        Object value = literal.getValue();
        if (value instanceof String text && text.length() > 65536) {
            return null;
        }
        if (Set.of("TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT").contains(type)
                && !tags.isEmpty() && tags.stream().allMatch(t -> t == 16 || t == 18)
                && value instanceof Long) {
            return value;
        }
        if ("BOOLEAN".equals(type) && value instanceof Boolean) {
            return value;
        }
        if ("STRING".equals(type) && value instanceof String && ((String) value).indexOf(0) < 0) {
            return value;
        }
        if ("DATETIMEV2".equals(type) && value instanceof LocalDateTime datetime
                && datetime.getNano() % 1000000 == 0) {
            // BSON Date uses UTC epoch milliseconds regardless of the Doris session timezone.
            return Date.from(datetime.toInstant(ZoneOffset.UTC));
        }
        if ("BIGINT".equals(type) && tags.equals(List.of(9)) && value instanceof Long millis) {
            return new Date(millis);
        }
        if ("DECIMALV3".equals(type) && (value instanceof BigDecimal || value instanceof Long)) {
            try {
                return new Decimal128(value instanceof BigDecimal
                        ? (BigDecimal) value : BigDecimal.valueOf((Long) value));
            } catch (NumberFormatException | ArithmeticException e) {
                return null;
            }
        }
        // DOUBLE/NaN comparisons and implicit coercions have no verified cross-engine contract.
        return null;
    }

    static String requiredLikeLiteral(String pattern) {
        if (pattern.length() > 4096 || pattern.indexOf(0) >= 0) {
            return null;
        }
        StringBuilder chunk = new StringBuilder();
        String longest = "";
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                if (++i == pattern.length() || "%_\\".indexOf(pattern.charAt(i)) < 0) {
                    return null;
                }
                chunk.append(pattern.charAt(i));
            } else if (c == '%' || c == '_') {
                if (chunk.length() > longest.length()) {
                    longest = chunk.toString();
                }
                chunk.setLength(0);
            } else {
                chunk.append(c);
            }
        }
        if (chunk.length() > longest.length()) {
            longest = chunk.toString();
        }
        if (longest.isEmpty()) {
            return null;
        }
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < longest.length(); i++) {
            char c = longest.charAt(i);
            if ("\\.^$|?*+()[]{}".indexOf(c) >= 0) {
                regex.append('\\');
            }
            regex.append(c);
        }
        // A required literal substring avoids PCRE2/RE2 wildcard, newline and Unicode-width differences.
        return regex.toString();
    }
}
