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

public class OptLogicalSelect extends OptLogical {

    public OptLogicalSelect() {
        super(OptOperatorType.OP_LOGICAL_SELECT);
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        return null;
    }

    @Override
    public Statistics deriveStat(
            OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        return StatisticsEstimator.estimateSelect(exprHandle, property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle, RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());

        // Predicates
        for (int i = 1; i < expressionHandle.arity(); i++) {
            final OptItemProperty childProperty =  expressionHandle.getChildItemProperty(i);
            columns.include(childProperty.getUsedColumns());
        }

        final OptLogicalProperty childProperty = expressionHandle.getChildLogicalProperty(0);
        columns.intersects(childProperty.getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        return getOutputColumnPassThrough(exprHandle);
    }
}
