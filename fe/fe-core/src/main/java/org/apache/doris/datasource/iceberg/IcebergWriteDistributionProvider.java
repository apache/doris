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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.ExternalWriteDistributionPlan;
import org.apache.doris.datasource.ExternalWriteDistributionProvider;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IcebergPartitionTransform;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IcebergPartitionTransform.Transform;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Plans transform-aware adaptive routing for Iceberg table writers. */
public final class IcebergWriteDistributionProvider
        implements ExternalWriteDistributionProvider<Table> {
    private static final Pattern PARAMETERIZED_TRANSFORM =
            Pattern.compile("(bucket|truncate)\\[(\\d+)\\]");
    private static final String ROUTING_COLUMN_PREFIX = "__doris_write_route_iceberg_";

    @Override
    public ExternalWriteDistributionPlan plan(Table table, List<Slot> sinkOutput) {
        Map<String, Slot> outputByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Slot slot : sinkOutput) {
            if (outputByName.put(slot.getName(), slot) != null) {
                return ExternalWriteDistributionPlan.singleWriter(
                        "duplicate Iceberg sink output column: " + slot.getName());
            }
        }

        ImmutableList.Builder<NamedExpression> routingExpressions = ImmutableList.builder();
        ImmutableMap.Builder<ExprId, Long> cardinalityCaps = ImmutableMap.builder();
        for (PartitionField field : table.spec().fields()) {
            String transformName = field.transform().toString().toLowerCase(Locale.ROOT);
            if ("void".equals(transformName)) {
                continue;
            }
            Types.NestedField sourceField = table.schema().findField(field.sourceId());
            if (sourceField == null) {
                return ExternalWriteDistributionPlan.singleWriter(
                        "missing Iceberg partition source field id " + field.sourceId());
            }
            Slot source = outputByName.get(sourceField.name());
            if (source == null) {
                return ExternalWriteDistributionPlan.singleWriter(
                        "missing Iceberg partition source column " + sourceField.name());
            }

            Expression routingExpression = createTransform(transformName, source);
            if (routingExpression == null) {
                return ExternalWriteDistributionPlan.singleWriter(
                        "unsupported Iceberg routing transform " + transformName
                                + " for " + source.getDataType());
            }
            Alias route = new Alias(routingExpression, ROUTING_COLUMN_PREFIX + field.fieldId());
            routingExpressions.add(route);
            Long cardinalityCap = cardinalityCap(transformName);
            if (cardinalityCap != null) {
                cardinalityCaps.put(route.getExprId(), cardinalityCap);
            }
        }

        List<NamedExpression> expressions = routingExpressions.build();
        if (expressions.isEmpty()) {
            return ExternalWriteDistributionPlan.random();
        }
        return ExternalWriteDistributionPlan.adaptiveHash(expressions, cardinalityCaps.build());
    }

    private Long cardinalityCap(String transformName) {
        Matcher matcher = PARAMETERIZED_TRANSFORM.matcher(transformName);
        if (matcher.matches() && "bucket".equals(matcher.group(1))) {
            return Long.parseLong(matcher.group(2));
        }
        return null;
    }

    private Expression createTransform(String transformName, Slot source) {
        if ("identity".equals(transformName)) {
            return source;
        }

        DataType sourceType = source.getDataType();
        switch (transformName) {
            case "year":
                return supportsDateTransform(sourceType)
                        ? new IcebergPartitionTransform(Transform.YEAR, source)
                        : null;
            case "month":
                return supportsDateTransform(sourceType)
                        ? new IcebergPartitionTransform(Transform.MONTH, source)
                        : null;
            case "day":
                return supportsDateTransform(sourceType)
                        ? new IcebergPartitionTransform(Transform.DAY, source)
                        : null;
            case "hour":
                return sourceType.isDateTimeV2Type()
                        ? new IcebergPartitionTransform(Transform.HOUR, source)
                        : null;
            default:
                return createParameterizedTransform(transformName, source, sourceType);
        }
    }

    private Expression createParameterizedTransform(
            String transformName, Slot source, DataType sourceType) {
        Matcher matcher = PARAMETERIZED_TRANSFORM.matcher(transformName);
        if (!matcher.matches()) {
            return null;
        }
        int width = Integer.parseInt(matcher.group(2));
        if (width <= 0) {
            return null;
        }
        if ("bucket".equals(matcher.group(1))) {
            return supportsBucket(sourceType)
                    ? new IcebergPartitionTransform(Transform.BUCKET, source, width)
                    : null;
        }
        return supportsTruncate(sourceType)
                ? new IcebergPartitionTransform(Transform.TRUNCATE, source, width)
                : null;
    }

    private boolean supportsDateTransform(DataType type) {
        return type.isDateV2Type() || type.isDateTimeV2Type();
    }

    private boolean supportsBucket(DataType type) {
        return type.isIntegerType()
                || type.isBigIntType()
                || type.isStringLikeType()
                || type.isDateV2Type()
                || type.isDateTimeV2Type()
                || type.isDecimalLikeType();
    }

    private boolean supportsTruncate(DataType type) {
        return type.isIntegerType()
                || type.isBigIntType()
                || type.isStringLikeType()
                || type.isDecimalLikeType();
    }
}
