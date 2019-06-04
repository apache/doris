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

package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptItemProperty;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

import java.util.BitSet;

public class OptLogicalProject extends OptLogical {

    public OptLogicalProject() {
        super(OptOperatorType.OP_LOGICAL_PROJECT);
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        return null;
    }

    @Override
    public Statistics deriveStat(
            OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        return StatisticsEstimator.estimateProject(exprHandle, property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle exprHandle, RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        final OptItemProperty project = exprHandle.getChildItemProperty(1);
        columns.intersects(project.getUsedColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        final OptLogicalProperty childProperty = exprHandle.getChildLogicalProperty(0);
        final OptItemProperty project = exprHandle.getChildItemProperty(1);
        columns.include(childProperty.getOutputColumns());
        columns.intersects(project.getUsedColumns());
        columns.include(project.getGeneratedColumns());
        return columns;
    }

}
