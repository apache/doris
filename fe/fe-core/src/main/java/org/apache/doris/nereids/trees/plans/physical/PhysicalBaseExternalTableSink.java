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

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** abstract physical external table sink */
public abstract class PhysicalBaseExternalTableSink<CHILD_TYPE extends Plan> extends PhysicalTableSink<CHILD_TYPE>
        implements Sink {

    protected final ExternalDatabase database;
    protected final ExternalTable targetTable;
    protected final List<Column> cols;

    /**
     * constructor
     */
    public PhysicalBaseExternalTableSink(PlanType type,
                                         ExternalDatabase database,
                                         ExternalTable targetTable,
                                         List<Column> cols,
                                         List<NamedExpression> outputExprs,
                                         Optional<GroupExpression> groupExpression,
                                         LogicalProperties logicalProperties,
                                         PhysicalProperties physicalProperties,
                                         Statistics statistics,
                                         CHILD_TYPE child) {
        super(type, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.database = Objects.requireNonNull(
                database, "database != null in " + this.getClass().getSimpleName());
        this.targetTable = Objects.requireNonNull(
                targetTable, "targetTable != null in " + this.getClass().getSimpleName());
        this.cols = Utils.copyRequiredList(cols);
    }

    public ExternalTable getTargetTable() {
        return targetTable;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

}
