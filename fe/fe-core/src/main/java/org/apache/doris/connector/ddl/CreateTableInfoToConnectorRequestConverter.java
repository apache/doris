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

package org.apache.doris.connector.ddl;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.ConnectorSortField;
import org.apache.doris.datasource.ConnectorColumnConverter;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SortFieldInfo;
import org.apache.doris.nereids.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Converts a nereids {@link CreateTableInfo} into a connector-SPI
 * {@link ConnectorCreateTableRequest}.
 *
 * <p>Covers Hive-style {@code IDENTITY}, Iceberg-style {@code TRANSFORM}, and
 * Doris {@code LIST} / {@code RANGE} partitioning, plus hash / random
 * distribution.</p>
 */
public final class CreateTableInfoToConnectorRequestConverter {

    private CreateTableInfoToConnectorRequestConverter() {
    }

    /**
     * @param info   the nereids CREATE TABLE info (must be analyzed)
     * @param dbName target database name (caller may normalize case)
     */
    public static ConnectorCreateTableRequest convert(CreateTableInfo info,
            String dbName) {
        return ConnectorCreateTableRequest.builder()
                .dbName(dbName)
                .tableName(info.getTableName())
                .columns(convertColumns(info.getColumnDefinitions()))
                .partitionSpec(convertPartition(info.getPartitionTableInfo()))
                .bucketSpec(convertBucket(info.getDistribution()))
                .sortOrder(convertSortOrder(info.getSortOrderFields()))
                .comment(info.getComment())
                .properties(info.getProperties())
                .ifNotExists(info.isIfNotExists())
                .external(info.isExternal())
                .build();
    }

    // -------- columns --------

    private static List<ConnectorColumn> convertColumns(
            List<ColumnDefinition> defs) {
        if (defs == null || defs.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumn> out = new ArrayList<>(defs.size());
        for (ColumnDefinition d : defs) {
            DataType nereidsType = d.getType();
            ConnectorType type = ConnectorColumnConverter.toConnectorType(
                    nereidsType.toCatalogDataType());
            // Mirror Column.isAggregated(): a non-null, non-NONE aggregate type means the user
            // wrote an aggregate column (e.g. SUM). The connector rejects these for engines that
            // cannot store them (MaxCompute); see MaxComputeConnectorMetadata.validateColumns.
            boolean isAggregated = d.getAggType() != null
                    && d.getAggType() != AggregateType.NONE;
            // Default value is not exposed via a public getter on ColumnDefinition
            // (private Optional<DefaultValue>); pass null until the SPI gains a
            // typed default-value carrier. See HANDOFF open issues.
            out.add(new ConnectorColumn(
                    d.getName(), type, d.getComment(),
                    d.isNullable(), null, d.isKey(), d.getAutoIncInitValue() != -1,
                    isAggregated));
        }
        return out;
    }

    // -------- partition --------

    private static ConnectorPartitionSpec convertPartition(
            PartitionTableInfo info) {
        if (info == null) {
            return null;
        }
        String pType = info.getPartitionType();
        List<Expression> exprs = info.getPartitionList();
        boolean isList = PartitionType.LIST.name().equalsIgnoreCase(pType);
        boolean isRange = PartitionType.RANGE.name().equalsIgnoreCase(pType);
        boolean hasExprs = exprs != null && !exprs.isEmpty();
        if (!isList && !isRange && !hasExprs) {
            return null;
        }

        ConnectorPartitionSpec.Style style;
        if (isList) {
            style = ConnectorPartitionSpec.Style.LIST;
        } else if (isRange) {
            style = ConnectorPartitionSpec.Style.RANGE;
        } else if (hasAnyTransform(exprs)) {
            style = ConnectorPartitionSpec.Style.TRANSFORM;
        } else {
            style = ConnectorPartitionSpec.Style.IDENTITY;
        }

        List<ConnectorPartitionField> fields = hasExprs
                ? convertFields(exprs)
                : Collections.emptyList();
        // LIST/RANGE PartitionDefinition values are not lowered here: each
        // PartitionDefinition is a sealed family (InPartition/LessThanPartition/
        // FixedRangePartition/StepPartition) carrying nereids Expressions that
        // require full analysis to flatten into List<List<String>>. Connectors
        // that need the initial values today read the Doris PartitionDesc
        // directly; this converter passes an empty list and leaves richer
        // lowering for a follow-up.
        return new ConnectorPartitionSpec(style, fields, Collections.emptyList());
    }

    private static boolean hasAnyTransform(List<Expression> exprs) {
        for (Expression e : exprs) {
            if (e instanceof UnboundFunction) {
                return true;
            }
        }
        return false;
    }

    private static List<ConnectorPartitionField> convertFields(
            List<Expression> exprs) {
        List<ConnectorPartitionField> out = new ArrayList<>(exprs.size());
        for (Expression e : exprs) {
            if (e instanceof UnboundSlot) {
                out.add(new ConnectorPartitionField(
                        ((UnboundSlot) e).getName(), "identity",
                        Collections.emptyList()));
            } else if (e instanceof UnboundFunction) {
                out.add(convertTransformField((UnboundFunction) e));
            }
            // Unknown expression shapes are dropped; the connector can still
            // honor the spec via its own analysis if richer info is required.
        }
        return out;
    }

    private static ConnectorPartitionField convertTransformField(
            UnboundFunction fn) {
        String transform = fn.getName().toLowerCase();
        String columnName = null;
        List<Integer> args = new ArrayList<>();
        for (Expression child : fn.children()) {
            if (child instanceof UnboundSlot && columnName == null) {
                columnName = ((UnboundSlot) child).getName();
            } else if (child instanceof IntegerLikeLiteral) {
                args.add(((IntegerLikeLiteral) child).getIntValue());
            } else if (child instanceof Literal) {
                Object v = ((Literal) child).getValue();
                if (v instanceof Number) {
                    args.add(((Number) v).intValue());
                }
            }
        }
        if (columnName == null) {
            columnName = fn.toString();
        }
        return new ConnectorPartitionField(columnName, transform, args);
    }

    // -------- bucket --------

    private static ConnectorBucketSpec convertBucket(DistributionDescriptor d) {
        if (d == null) {
            return null;
        }
        List<String> cols = d.getCols() == null
                ? Collections.emptyList()
                : d.getCols();
        // bucketNum is private; read it off the translated catalog desc so we
        // do not depend on private internals.
        int numBuckets = readBucketNum(d);
        String algorithm = d.isHash() ? "doris_default" : "doris_random";
        return new ConnectorBucketSpec(cols, numBuckets, algorithm);
    }

    private static int readBucketNum(DistributionDescriptor d) {
        try {
            return d.translateToCatalogStyle().getBuckets();
        } catch (Exception ignored) {
            return 0;
        }
    }

    // -------- sort order --------

    /**
     * Carries the {@code ORDER BY (...)} write-order clause neutrally so a connector (iceberg) can build an
     * engine sort order. Iceberg-specific validation (column existence, no metric-only types, no duplicates)
     * already ran in fe-core {@code CreateTableInfo} before this conversion; here we only map the shape.
     */
    private static List<ConnectorSortField> convertSortOrder(List<SortFieldInfo> sortFields) {
        if (sortFields == null || sortFields.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorSortField> out = new ArrayList<>(sortFields.size());
        for (SortFieldInfo f : sortFields) {
            out.add(new ConnectorSortField(f.getColumnName(), f.isAscending(), f.isNullFirst()));
        }
        return out;
    }
}
