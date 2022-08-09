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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalRelation  {

    public LogicalOlapScan(Table table) {
        this(table, ImmutableList.of());
    }

    public LogicalOlapScan(Table table, List<String> qualifier) {
        this(table, qualifier, Optional.empty(), Optional.empty());
    }

    /**
     * Constructor for LogicalOlapScan.
     *
     * @param table Doris table
     * @param qualifier table name qualifier
     */
    public LogicalOlapScan(Table table, List<String> qualifier,
                           Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_OLAP_SCAN, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public String toString() {
        return "ScanOlapTable ("
                + qualifiedName()
                + ", output: "
                + computeOutput().stream().map(Objects::toString).collect(Collectors.joining(", ", "[",  "]"))
                + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }
        return true;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(table, qualifier, groupExpression, Optional.of(logicalProperties));
    }

    @Override
    public LogicalOlapScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalOlapScan(table, qualifier, Optional.empty(), logicalProperties);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }
}
