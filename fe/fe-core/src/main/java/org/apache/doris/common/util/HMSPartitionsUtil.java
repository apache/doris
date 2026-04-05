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

package org.apache.doris.common.util;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HMSPartitionsUtil {
    /**
     *  check selected partition num limit
     *
     */
    public static void checkSelectedPartitionNumLimit(HMSExternalTable hmsTable,
                                                      int selectedPartitionNum) throws AnalysisException {
        if (hmsTable.getDlaType() == HMSExternalTable.DLAType.HUDI) {
            if (selectedPartitionNum > Config.max_selected_partition_num_for_lakehouse_table) {
                throw new AnalysisException("the selected partition num: "
                    + selectedPartitionNum + " for " + hmsTable.getDbName() + "."
                    + hmsTable.getName() + " has "
                    + "exceed max selected partition num for single hudi table: "
                    + Config.max_selected_partition_num_for_lakehouse_table);
            }
        } else {
            if (selectedPartitionNum > Config.max_selected_partition_num_for_hive_table) {
                throw new AnalysisException("the selected partition num: " + selectedPartitionNum
                    + " for " + hmsTable.getDbName() + "." + hmsTable.getName() + " has "
                    + "exceed max selected partition num for single hive table: "
                    + Config.max_selected_partition_num_for_hive_table);
            }
        }
    }

    public static void checkSelectedFileNumLimit(HMSExternalTable hmsTable, int selectedFileNum)
            throws AnalysisException {
        if (selectedFileNum > Config.max_selected_total_file_num_for_hive_table) {
            throw new AnalysisException("the selected file num: "
                + selectedFileNum + " for " + hmsTable.getDbName() + "."
                + hmsTable.getName() + " has "
                + "exceed max selected file num for single hive table: "
                + Config.max_selected_total_file_num_for_hive_table);
        }
    }

    public static void checkSelectedSplitNumLimit(HMSExternalTable hmsTable, int selectedSplitNum)
            throws AnalysisException {
        if (selectedSplitNum > Config.max_selected_total_split_num_for_hms_table) {
            throw new AnalysisException("the selected split num: "
                + selectedSplitNum + " for " + hmsTable.getDbName() + "."
                + hmsTable.getName() + " has "
                + "exceed max selected split num for single hms table: "
                + Config.max_selected_total_split_num_for_hms_table);
        }
    }


    public static boolean isFilterSupportedByListPartitions(Expression expression) {
        if (expression instanceof SlotReference) {
            return true;
        }
        if (expression instanceof Literal
                && !(expression instanceof org.apache.doris.nereids.trees.expressions.literal.NullLiteral)) {
            return true;
        }
        if (expression instanceof And || expression instanceof Or || expression instanceof EqualTo
                || expression instanceof GreaterThan || expression instanceof GreaterThanEqual
                || expression instanceof LessThan || expression instanceof LessThanEqual) {
            if (expression instanceof BinaryOperator) {
                BinaryOperator binaryOperator = (BinaryOperator) expression;
                if (binaryOperator.right() instanceof SlotReference) {
                    return false;
                }
                return isFilterSupportedByListPartitions(binaryOperator.left())
                    && isFilterSupportedByListPartitions(binaryOperator.right());
            } else {
                for (Expression child : expression.children()) {
                    if (!isFilterSupportedByListPartitions(child)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Build constant expressions list from partition values with type coercion.
     * This integrates the logic from MergeOneRowRelationIntoUnion to directly create
     * a union with constantExprsList instead of creating LogicalOneRowRelation children.
     */
    public static List<List<NamedExpression>> buildConstantExpressionsFromPartitions(
            Collection<PartitionItem> selectedPartitions,
            List<Column> partitionColumns,
            Map<String, DataType> nameToType,
            List<NamedExpression> unionOutputs) {
        ImmutableList.Builder<List<NamedExpression>> constantExprsList = ImmutableList.builder();
        for (PartitionItem partitionItem : selectedPartitions) {
            PartitionKey partitionKey = ((ListPartitionItem) partitionItem).getItems().get(0);
            ImmutableList.Builder<NamedExpression> constantExprs = ImmutableList.builder();

            for (int i = 0; i < partitionColumns.size(); i++) {
                String name = partitionColumns.get(i).getName();
                if (nameToType.containsKey(name)) {
                    Literal literal;
                    LiteralExpr literalExpr = partitionKey.getKeys().get(i);
                    if (literalExpr instanceof DateLiteral) {
                        if (literalExpr.getType().equals(Type.DATE) || literalExpr.getType().equals(Type.DATEV2)) {
                            literal = new DateV2Literal(literalExpr.getStringValue());
                        } else {
                            literal = new DateTimeV2Literal(literalExpr.getStringValue());
                        }
                    } else {
                        literal = partitionKey.getKeys().get(i) instanceof NullLiteral
                            ? Literal.of(null)
                            : Literal.of(partitionKey.getKeys().get(i).getRealValue());
                    }
                    Expression castedLiteral = literal.checkedCastTo(nameToType.get(name));

                    // Find the corresponding output to get target type for proper coercion
                    DataType targetType = null;
                    for (NamedExpression output : unionOutputs) {
                        if (output.getName().equals(name)) {
                            targetType = output.getDataType();
                            break;
                        }
                    }

                    // Apply type coercion like @{MergeOneRowRelationIntoUnion} does
                    if (targetType != null && !castedLiteral.getDataType().equals(targetType)) {
                        castedLiteral = TypeCoercionUtils.castIfNotSameType(castedLiteral, targetType);
                    }

                    constantExprs.add(new Alias(castedLiteral, name));
                }
            }
            constantExprsList.add(constantExprs.build());
        }

        return constantExprsList.build();
    }

    public static Map<String, PartitionItem> getPartitionItems(HiveMetaStoreCache cache, HMSExternalTable hiveTable,
                                                               int partitionNum) {
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = partitionNum
                > Config.max_partition_num_for_hive_table_with_partition_cache
                ? cache.getPartitionValuesWithoutCache(hiveTable, hiveTable.getPartitionColumnTypes()) :
                cache.getPartitionValues(hiveTable, hiveTable.getPartitionColumnTypes());
        Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
        BiMap<Long, String> idToName = hivePartitionValues.getPartitionNameToIdMap().inverse();
        Map<String, PartitionItem> partitionItems = Maps.newHashMapWithExpectedSize(idToPartitionItem.size());
        for (Map.Entry<Long, PartitionItem> entry : idToPartitionItem.entrySet()) {
            partitionItems.put(idToName.get(entry.getKey()), entry.getValue());
        }
        return partitionItems;
    }
}
