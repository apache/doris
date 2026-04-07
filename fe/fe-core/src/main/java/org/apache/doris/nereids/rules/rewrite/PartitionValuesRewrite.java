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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.util.HMSPartitionsUtil;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.ImplementationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.table.PartitionValues;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * implement partition values rewrite rule
 */
public class PartitionValuesRewrite implements ImplementationRuleFactory {

    static boolean canApplyPartitionValuesRewrite(LogicalFilter<LogicalTVFRelation> filter) {
        LogicalTVFRelation relation = filter.child();
        return relation.getFunction() instanceof PartitionValues;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(RuleType.PARTITION_VALUES_WITH_FILTER_FOR_TVF_RELATION.build(logicalFilter(
                logicalTVFRelation()
            ).when(PartitionValuesRewrite::canApplyPartitionValuesRewrite)
                .thenApply(ctx -> {
                    LogicalFilter<LogicalTVFRelation> filter = ctx.root;
                    LogicalTVFRelation relation = filter.child();
                    PartitionValuesTableValuedFunction valuedFunction =
                            (PartitionValuesTableValuedFunction) relation.getFunction().getCatalogFunction();
                    HMSExternalTable hiveTable = (HMSExternalTable) valuedFunction.getPartitionedTable();
                    List<Column> partitionColumns = hiveTable.getPartitionColumns();
                    Set<Expression> conjunctsForPushDown = Sets.newHashSet();
                    Set<Expression> remainedConjuncts = Sets.newHashSet();
                    for (Expression expression : filter.getConjuncts()) {
                        if (HMSPartitionsUtil.isFilterSupportedByListPartitions(expression)) {
                            conjunctsForPushDown.add(expression);
                        } else {
                            remainedConjuncts.add(expression);
                        }
                    }
                    HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                            .getMetaStoreCache((HMSExternalCatalog) hiveTable.getCatalog());
                    Map<String, PartitionItem> selectedPartitions;
                    if (!conjunctsForPushDown.isEmpty()) {
                        List<String> partitionColumnNames = partitionColumns.stream().map(Column::getName)
                                .collect(Collectors.toList());
                        selectedPartitions = cache.getPartitionValuesByFilter(hiveTable,
                                ExpressionUtils.and(conjunctsForPushDown).toSql(), partitionColumnNames,
                                hiveTable.getPartitionColumnTypes());
                    } else {
                        int partitionNum = cache.getPartitionNum(hiveTable);
                        selectedPartitions = HMSPartitionsUtil.getPartitionItems(cache, hiveTable, partitionNum);
                    }
                    Map<String, DataType> nameToType = Maps.newHashMap();
                    for (NamedExpression expression : relation.getOutput()) {
                        nameToType.put(expression.getName(), expression.getDataType());
                    }

                    // Handle empty partition selection case - return empty relation
                    if (selectedPartitions.isEmpty()) {
                        return filter.withChildren(ImmutableList.of(
                            new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(), relation.getOutput())));
                    }

                    List<NamedExpression> unionOutputs = new ArrayList<>(relation.getOutput());

                    List<List<NamedExpression>> constantExprsList =
                            HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                            selectedPartitions.values(), partitionColumns, nameToType, unionOutputs);

                    // Create LogicalUnion with constantExprsList instead of children
                    LogicalUnion union = new LogicalUnion(Qualifier.ALL, unionOutputs,
                            ImmutableList.of(), constantExprsList, false, ImmutableList.of());

                    if (remainedConjuncts.isEmpty()) {
                        return union;
                    } else {
                        return filter.withConjunctsAndChild(remainedConjuncts, union);
                    }
                })),
            RuleType.PARTITION_VALUES_WITHOUT_FILTER_FOR_TVF_RELATION.build(logicalTVFRelation(
            ).when(relation -> relation.getFunction() instanceof PartitionValues).thenApply(
                ctx -> {
                    LogicalTVFRelation relation = ctx.root;
                    PartitionValuesTableValuedFunction valuedFunction
                            = (PartitionValuesTableValuedFunction) relation.getFunction().getCatalogFunction();
                    HMSExternalTable hiveTable = (HMSExternalTable) valuedFunction.getPartitionedTable();
                    List<Column> partitionColumns = hiveTable.getPartitionColumns();
                    HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                            .getMetaStoreCache((HMSExternalCatalog) hiveTable.getCatalog());
                    int partitionNum = cache.getPartitionNum(hiveTable);
                    Map<String, PartitionItem> selectedPartitions
                            = HMSPartitionsUtil.getPartitionItems(cache, hiveTable, partitionNum);
                    Map<String, DataType> nameToType = Maps.newHashMap();
                    for (NamedExpression expression : relation.getOutput()) {
                        nameToType.put(expression.getName(), expression.getDataType());
                    }

                    // Handle empty partition selection case - return empty relation
                    if (selectedPartitions.isEmpty()) {
                        return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(), relation.getOutput());
                    }

                    List<NamedExpression> unionOutputs = new ArrayList<>(relation.getOutput());

                    List<List<NamedExpression>> constantExprsList =
                            HMSPartitionsUtil.buildConstantExpressionsFromPartitions(selectedPartitions.values(),
                                partitionColumns, nameToType, unionOutputs);

                    // Create LogicalUnion with constantExprsList instead of children
                    return new LogicalUnion(Qualifier.ALL, unionOutputs,
                            ImmutableList.of(), constantExprsList, false, ImmutableList.of());
                }
            ))
        );
    }
}
