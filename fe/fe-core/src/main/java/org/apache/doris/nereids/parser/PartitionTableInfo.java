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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.StepPartition;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * partition info for 'PARTITION BY'
 */
public class PartitionTableInfo {

    public static final PartitionTableInfo EMPTY = new PartitionTableInfo(
                false,
                PartitionType.UNPARTITIONED.name(),
                null,
                null);

    private boolean isAutoPartition;
    // for PartitionType
    private String partitionType;
    private List<PartitionDefinition> partitionDefs;
    // save all list partition expressions, including identifier and function
    private List<Expression> partitionList;
    // save identifier expressions in partitionList,
    // facilitates subsequent verification process
    private List<String> identifierPartitionColumns;

    /**
     * struct for partition definition
     *
     * @param isAutoPartition Whether it is an automatic partition
     * @param partitionType partition type
     * @param partitionFields partition fields
     */
    public PartitionTableInfo(
            boolean isAutoPartition,
            String partitionType,
            List<PartitionDefinition> partitionDefs,
            List<Expression> partitionFields) {
        this.isAutoPartition = isAutoPartition;
        this.partitionType = partitionType;
        this.partitionDefs = partitionDefs;
        this.partitionList = partitionFields;
        if (this.partitionList != null) {
            this.identifierPartitionColumns = this.partitionList.stream()
                .filter(UnboundSlot.class::isInstance)
                .map(partition -> ((UnboundSlot) partition).getName())
                .collect(Collectors.toList());
        }
    }

    public boolean isAutoPartition() {
        return isAutoPartition;
    }

    public String getPartitionType() {
        return partitionType;
    }

    /**
     * check partitions types.
     */
    private boolean checkPartitionsTypes() {
        if (partitionType.equalsIgnoreCase(PartitionType.RANGE.name())) {
            if (partitionDefs.stream().allMatch(
                    p -> p instanceof StepPartition || p instanceof FixedRangePartition)) {
                return true;
            }
            return partitionDefs.stream().allMatch(
                p -> (p instanceof LessThanPartition) || (p instanceof FixedRangePartition));
        }
        return partitionType.equalsIgnoreCase(PartitionType.LIST.name())
            && partitionDefs.stream().allMatch(p -> p instanceof InPartition);
    }

    private void validatePartitionColumn(ColumnDefinition column, ConnectContext ctx,
                                         boolean isEnableMergeOnWrite, boolean isExternal) {
        if (!column.isKey()) { // value column
            if (!column.getAggType().equals(AggregateType.NONE)) { // agg column
                throw new AnalysisException("The partition column could not be aggregated column");
            }
            if (isEnableMergeOnWrite) { // MoW table
                throw new AnalysisException("Merge-on-Write table's partition column must be KEY column");
            }
        }
        if (column.getType().isFloatLikeType()) {
            throw new AnalysisException("Floating point type column can not be partition column");
        }
        if (column.getType().isStringType() && !isExternal) {
            throw new AnalysisException("String Type should not be used in partition column["
                + column.getName() + "].");
        }
        if (column.getType().isComplexType()) {
            throw new AnalysisException("Complex type column can't be partition column: "
                + column.getType().toString());
        }
        if (!ctx.getSessionVariable().isAllowPartitionColumnNullable() && column.isNullable()) {
            throw new AnalysisException(
                "The partition column must be NOT NULL with allow_partition_column_nullable OFF");
        }
        if (partitionType.equalsIgnoreCase(PartitionType.RANGE.name()) && isAutoPartition) {
            if (column.isNullable()) {
                throw new AnalysisException("AUTO RANGE PARTITION doesn't support NULL column");
            }
        }
    }

    /**
     * Verify the relationship between partitions and columns
     *
     * @param columnMap column map of table
     * @param properties properties of table
     * @param ctx context
     * @param isEnableMergeOnWrite whether enable merge on write
     */
    public void validatePartitionInfo(
            String engineName,
            List<ColumnDefinition> columns,
            Map<String, ColumnDefinition> columnMap,
            Map<String, String> properties,
            ConnectContext ctx,
            boolean isEnableMergeOnWrite,
            boolean isExternal) {

        if (identifierPartitionColumns != null) {

            if (identifierPartitionColumns.size() != partitionList.size()) {
                if (!isExternal && partitionType.equalsIgnoreCase(PartitionType.LIST.name())) {
                    throw new AnalysisException("internal catalog does not support functions in 'LIST' partition");
                }
                isAutoPartition = true;
            }

            identifierPartitionColumns.forEach(p -> {
                if (!columnMap.containsKey(p)) {
                    throw new AnalysisException(
                            String.format("partition key %s is not exists", p));
                }
                validatePartitionColumn(columnMap.get(p), ctx, isEnableMergeOnWrite, isExternal);
            });

            Set<String> partitionColumnSets = Sets.newHashSet();
            List<String> duplicatesKeys = identifierPartitionColumns.stream()
                    .filter(c -> !partitionColumnSets.add(c)).collect(Collectors.toList());
            if (!duplicatesKeys.isEmpty()) {
                throw new AnalysisException(
                        "Duplicated partition column " + duplicatesKeys.get(0));
            }

            if (engineName.equals(CreateTableInfo.ENGINE_HIVE)) {
                // 1. Cannot set all columns as partitioning columns
                // 2. The partition field must be at the end of the schema
                // 3. The order of partition fields in the schema
                //    must be consistent with the order defined in `PARTITIONED BY LIST()`
                if (identifierPartitionColumns.size() == columns.size()) {
                    throw new AnalysisException("Cannot set all columns as partitioning columns.");
                }
                List<ColumnDefinition> partitionInSchema = columns.subList(
                        columns.size() - identifierPartitionColumns.size(), columns.size());
                if (partitionInSchema.stream().anyMatch(p -> !identifierPartitionColumns.contains(p.getName()))) {
                    throw new AnalysisException("The partition field must be at the end of the schema.");
                }
                for (int i = 0; i < partitionInSchema.size(); i++) {
                    if (!partitionInSchema.get(i).getName().equals(identifierPartitionColumns.get(i))) {
                        throw new AnalysisException("The order of partition fields in the schema "
                            + "must be consistent with the order defined in `PARTITIONED BY LIST()`");
                    }
                }
            }

            if (partitionDefs != null) {
                if (!checkPartitionsTypes()) {
                    throw new AnalysisException(
                            "partitions types is invalid, expected FIXED or LESS in range partitions"
                                    + " and IN in list partitions");
                }
                Set<String> partitionNames = Sets.newHashSet();
                for (PartitionDefinition partition : partitionDefs) {
                    if (partition instanceof StepPartition) {
                        continue;
                    }
                    String partitionName = partition.getPartitionName();
                    if (partitionNames.contains(partitionName)) {
                        throw new AnalysisException(
                                "Duplicated named partition: " + partitionName);
                    }
                    partitionNames.add(partitionName);
                }
                partitionDefs.forEach(p -> {
                    p.setPartitionTypes(identifierPartitionColumns.stream()
                            .map(s -> columnMap.get(s).getType()).collect(Collectors.toList()));
                    p.validate(Maps.newHashMap(properties));
                });
            }
        }
    }

    /**
     *  Convert to PartitionDesc types.
     */
    public PartitionDesc convertToPartitionDesc(boolean isExternal) {
        PartitionDesc partitionDesc = null;
        if (isExternal) {
            isAutoPartition = true;
        }
        if (!partitionType.equalsIgnoreCase(PartitionType.UNPARTITIONED.name())) {
            List<AllPartitionDesc> partitionDescs =
                    partitionDefs != null
                    ? partitionDefs.stream().map(PartitionDefinition::translateToCatalogStyle)
                    .collect(Collectors.toList())
                    : null;

            int createTablePartitionMaxNum = ConnectContext.get().getSessionVariable().getCreateTablePartitionMaxNum();
            if (partitionDescs != null && partitionDescs.size() > createTablePartitionMaxNum) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(String.format(
                    "The number of partitions to be created is [%s], exceeding the maximum value of [%s]. "
                        + "Creating too many partitions can be time-consuming. If necessary, "
                        + "You can set the session variable 'create_table_partition_max_num' "
                        + "to a larger value.",
                    partitionDescs.size(), createTablePartitionMaxNum));
            }

            try {
                ArrayList<Expr> exprs = convertToLegacyAutoPartitionExprs(partitionList);

                // only auto partition support partition expr
                if (!isAutoPartition) {
                    if (exprs.stream().anyMatch(expr -> expr instanceof FunctionCallExpr)) {
                        throw new DdlException("Non-auto partition table not support partition expr!");
                    }
                }

                // here we have already extracted identifierPartitionColumns
                if (partitionType.equals(PartitionType.RANGE.name())) {
                    if (isAutoPartition) {
                        partitionDesc = new RangePartitionDesc(exprs, identifierPartitionColumns, partitionDescs);
                    } else {
                        partitionDesc = new RangePartitionDesc(identifierPartitionColumns, partitionDescs);
                    }
                } else {
                    if (isAutoPartition) {
                        partitionDesc = new ListPartitionDesc(exprs, identifierPartitionColumns, partitionDescs);
                    } else {
                        partitionDesc = new ListPartitionDesc(identifierPartitionColumns, partitionDescs);
                    }
                }
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }
        return partitionDesc;
    }

    private static ArrayList<Expr> convertToLegacyAutoPartitionExprs(List<Expression> expressions) {
        return new ArrayList<>(expressions.stream().map(expression -> {
            if (expression instanceof UnboundSlot) {
                return new SlotRef(null, ((UnboundSlot) expression).getName());
            } else if (expression instanceof UnboundFunction) {
                UnboundFunction function = (UnboundFunction) expression;
                return new FunctionCallExpr(
                        function.getName(),
                        new FunctionParams(convertToLegacyArguments(function.children())));
            } else {
                throw new AnalysisException(
                    "unsupported auto partition expr " + expression.toString());
            }
        }).collect(Collectors.toList()));
    }

    private static List<Expr> convertToLegacyArguments(List<Expression> children) {
        return children.stream().map(child -> {
            if (child instanceof UnboundSlot) {
                return new SlotRef(null, ((UnboundSlot) child).getName());
            } else if (child instanceof Literal) {
                return new StringLiteral(((Literal) child).getStringValue());
            } else {
                throw new AnalysisException("unsupported argument " + child.toString());
            }
        }).collect(Collectors.toList());
    }

    /**
     *  Get column names and put in identifierPartitionColumns
     */
    public void extractPartitionColumns() throws AnalysisException {
        if (partitionList == null) {
            return;
        }
        ArrayList<Expr> exprs = convertToLegacyAutoPartitionExprs(partitionList);
        try {
            identifierPartitionColumns = PartitionDesc.getColNamesFromExpr(exprs,
                    partitionType.equalsIgnoreCase(PartitionType.LIST.name()), isAutoPartition);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }

    public boolean inIdentifierPartitions(String columnName) {
        return identifierPartitionColumns != null && identifierPartitionColumns.contains(columnName);
    }
}
