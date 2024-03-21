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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.statistics.Statistics;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Physical table sink for all table type sink
 */
public abstract class PhysicalTableSink<CHILD_TYPE extends Plan> extends PhysicalSink<CHILD_TYPE> {

    public PhysicalTableSink(PlanType type, List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(type, outputExprs, groupExpression, logicalProperties, physicalProperties, statistics, child);
    }

    public abstract PhysicalProperties getRequirePhysicalProperties();

    public abstract TableIf getTargetTable();
}
