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

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

public class OptLogicalLimit extends OptLogical {

    private final int limit;
    private final int offset;

    protected OptLogicalLimit() {
        super(OptOperatorType.OP_LOGICAL_LIMIT);
        limit = -1;
        offset = -1;
    }

    protected OptLogicalLimit(int limit, int offset) {
        super(OptOperatorType.OP_LOGICAL_LIMIT);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        return getOutputColumnPassThrough(exprHandle);
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        return StatisticsEstimator.estimateLimit(exprHandle, property, limit);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle exprHandle, RequiredLogicalProperty property, int childIndex) {
        Preconditions.checkArgument(childIndex == 0);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());

        final OptLogicalProperty childProperty= exprHandle.getChildLogicalProperty(childIndex);
        columns.intersects(childProperty.getOutputColumns());
        return columns;
    }

    public int getLimit() { return limit; }
    public int getOffset() { return offset; }
}
