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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * adjust output for insert target type
 */
public class CheckSourceAndAdjustOutputForInsertTargetType implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        List<DataType> insertTargetTypes = jobContext.getCascadesContext()
                .getStatementContext().getInsertTargetSchema();
        if (insertTargetTypes == null) {
            // not insert into command, skip.
            return plan;
        }
        List<Slot> outputs = plan.getOutput();
        List<NamedExpression> newSlots = Lists.newArrayListWithCapacity(outputs.size());
        for (int i = 0; i < insertTargetTypes.size(); ++i) {
            if (insertTargetTypes.get(i).equals(outputs.get(i).getDataType())) {
                newSlots.add(outputs.get(i));
            } else {
                Slot slot = outputs.get(i);
                newSlots.add(new Alias(new Cast(slot, insertTargetTypes.get(i)), slot.getQualifiedName()));
            }
        }
        return new LogicalProject<>(newSlots, plan);
    }

    private void check(List<DataType> targetType, List<Slot> slots) {
        Preconditions.checkArgument(targetType.size() == slots.size(),
                String.format("insert target table contains %d slots, but source table contains %d slots",
                        targetType.size(), slots.size()));
        for (int i = 0; i < targetType.size(); i++) {
            if (!targetType.get(i).acceptsType(slots.get(i).getDataType())) {
                throw new AnalysisException(String.format("%s cannot be cast to %s",
                        slots.get(i).getDataType(), targetType.get(i)));
            }
        }

    }
}
