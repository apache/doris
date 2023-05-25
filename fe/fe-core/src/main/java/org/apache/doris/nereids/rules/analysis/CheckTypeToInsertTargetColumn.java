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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * adjust output for insert target type
 */
public class CheckTypeToInsertTargetColumn extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalOlapTableSink()).then(project -> {
            LogicalOlapTableSink<?> sink = project.child();
            List<DataType> insertTargetTypes = sink.getTargetTable().getFullSchema()
                    .stream().map(column -> DataType.fromCatalogType(column.getType()))
                    .collect(Collectors.toList());
            Optional<List<NamedExpression>> newOutput = checkNeedCast(insertTargetTypes, project.getOutputs());
            if (newOutput.isPresent()) {
                return project.withProjects(newOutput.get());
            } else {
                return project;
            }
        }).toRule(RuleType.CHECK_TYPE_TO_INSERT_TARGET_COLUMN);
    }

    private Optional<List<NamedExpression>> checkNeedCast(List<DataType> targetType, List<NamedExpression> slots) {
        Preconditions.checkArgument(targetType.size() == slots.size(),
                String.format("insert target table contains %d slots, but source table contains %d slots",
                        targetType.size(), slots.size()));
        boolean isNeedCast = false;
        for (int i = 0; i < slots.size(); ++i) {
            if (!targetType.get(i).equals(slots.get(i).getDataType())) {
                isNeedCast = true;
                break;
            }
        }
        if (!isNeedCast) {
            return Optional.empty();
        }
        List<NamedExpression> newOutput = Lists.newArrayList();
        for (int i = 0; i < slots.size(); ++i) {
            Expression ne = slots.get(i);
            if (ne instanceof Alias) {
                ne = ((Alias) ne).child();
            }
            Expression castExpression = TypeCoercionUtils.castIfNotSameType(ne, targetType.get(i));
            newOutput.add(castExpression instanceof NamedExpression
                    ? ((NamedExpression) castExpression)
                    : new Alias(castExpression, castExpression.toSql()));
        }
        return Optional.of(newOutput);
    }
}
