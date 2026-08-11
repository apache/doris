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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.common.UserException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.load.LoadDeleteOnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPartitionNames;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPrecedingFilterClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSequenceClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadWhereClause;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Snapshot of the current CREATE ROUTINE LOAD semantics.
 */
public class RoutineLoadDefinition {
    @SerializedName("desc")
    private RoutineLoadDesc routineLoadDesc;
    @SerializedName("jp")
    private Map<String, String> jobProperties = Maps.newHashMap();
    @SerializedName("dsp")
    private Map<String, String> dataSourceProperties = Maps.newHashMap();

    public RoutineLoadDefinition(RoutineLoadDesc routineLoadDesc,
            Map<String, String> jobProperties, Map<String, String> dataSourceProperties) {
        this.routineLoadDesc = routineLoadDesc;
        this.jobProperties.putAll(jobProperties);
        this.dataSourceProperties.putAll(dataSourceProperties);
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public Map<String, String> getDataSourceProperties() {
        return dataSourceProperties;
    }

    public CreateRoutineLoadInfo toCreateInfo(String dbName, String jobName, String tableName,
            LoadDataSourceType dataSourceType, String comment) throws UserException {
        LoadTask.MergeType mergeType = routineLoadDesc == null
                ? LoadTask.MergeType.APPEND : routineLoadDesc.getMergeType();
        return new CreateRoutineLoadInfo(new LabelNameInfo(dbName, jobName), tableName,
                toLoadPropertyMap(routineLoadDesc), Maps.newHashMap(jobProperties), dataSourceType.name(),
                Maps.newHashMap(dataSourceProperties), mergeType, comment);
    }

    private static Map<String, LoadProperty> toLoadPropertyMap(RoutineLoadDesc routineLoadDesc) throws UserException {
        Map<String, LoadProperty> loadProperties = Maps.newHashMap();
        if (routineLoadDesc == null) {
            return loadProperties;
        }
        if (routineLoadDesc.getColumnSeparator() != null) {
            put(loadProperties, new LoadSeparator(routineLoadDesc.getColumnSeparator().getOriSeparator()));
        }
        if (routineLoadDesc.getColumnsInfo() != null) {
            List<LoadColumnDesc> columns = new ArrayList<>();
            for (ImportColumnDesc column : routineLoadDesc.getColumnsInfo()) {
                Expression expression = column.getExpr() == null ? null : parseExpression(column.getExpr());
                columns.add(new LoadColumnDesc(column.getColumnName(), expression));
            }
            put(loadProperties, new LoadColumnClause(columns));
        }
        if (routineLoadDesc.getPrecedingFilter() != null) {
            put(loadProperties, new LoadPrecedingFilterClause(
                    parseExpression(routineLoadDesc.getPrecedingFilter())));
        }
        if (routineLoadDesc.getFilter() != null) {
            put(loadProperties, new LoadWhereClause(parseExpression(routineLoadDesc.getFilter())));
        }
        if (routineLoadDesc.getPartitionNamesInfo() != null) {
            put(loadProperties, new LoadPartitionNames(
                    routineLoadDesc.getPartitionNamesInfo().isTemp(),
                    routineLoadDesc.getPartitionNamesInfo().getPartitionNames()));
        }
        if (routineLoadDesc.getDeleteCondition() != null) {
            put(loadProperties, new LoadDeleteOnClause(parseExpression(routineLoadDesc.getDeleteCondition())));
        }
        if (routineLoadDesc.hasSequenceCol()) {
            put(loadProperties, new LoadSequenceClause(routineLoadDesc.getSequenceColName()));
        }
        return loadProperties;
    }

    private static Expression parseExpression(Expr expression) throws UserException {
        String sql = expression.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
        return NereidsLoadUtils.parseExpressionSeq(sql).get(0);
    }

    private static void put(Map<String, LoadProperty> loadProperties, LoadProperty loadProperty) {
        loadProperties.put(loadProperty.getClass().getName(), loadProperty);
    }
}
