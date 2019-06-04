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
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.stat.Statistics;

public class OptLogicalJoin extends OptLogical {

    public OptLogicalJoin() {
        super(OptOperatorType.OP_LOGICAL_JOIN);
    }

    public OptLogicalJoin(OptOperatorType type) {
        super(type);
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        return new Statistics(property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle exprHandle, RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());

        // Join conjuncs.
        for (int i = 2; i < exprHandle.arity() - 1; i++) {
            final OptItemProperty childProperty = exprHandle.getChildItemProperty(i);
            columns.include(childProperty.getUsedColumns());
        }

        columns.intersects(exprHandle.getChildLogicalProperty(childIndex).getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        OptColumnRefSet columns = new OptColumnRefSet();
        for (int i = 0; i < exprHandle.arity() - 1; ++i) {
            columns.include(exprHandle.getChildLogicalProperty(i).getOutputColumns());
        }
        return columns;
    }

}
