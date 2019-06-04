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

package org.apache.doris.optimizer.base;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.operator.*;

public class OptItemProperty implements OptProperty {
    private final OptColumnRefSet usedColumns;
    private final OptColumnRefSet generatedColumns;
    // These are used for OptItemFunctionCall and OptItemProjectElement.
    private boolean isDistinctAgg;
    private boolean isDisintctAggMultiParam;

    // These are used for OptItemProjectList.
    private int distinctAggCount;
    private boolean isMultiAggDistinct;

    public OptItemProperty() {
        this.usedColumns = new OptColumnRefSet();
        this.generatedColumns = new OptColumnRefSet();
        this.distinctAggCount = 0;
        this.isDistinctAgg = false;
        this.isMultiAggDistinct = false;
    }

    public OptColumnRefSet getUsedColumns() { return usedColumns; }

    public OptColumnRefSet getGeneratedColumns() { return generatedColumns; }

    public boolean isDistinctAgg() { return isDistinctAgg; }

    public boolean isMultiAggDistinct() { return isMultiAggDistinct; }

    public boolean isHavingSubQuery() {
        return false;
    }

    @Override
    public void derive(OptExpressionHandle exprHandle) {
        Preconditions.checkArgument(exprHandle.getOp() instanceof OptItem,
                "op is not item, op=" + exprHandle.getOp());

        final OptItem item = (OptItem) exprHandle.getOp();
        usedColumns.include(item.getUsedColumns());
        generatedColumns.include(item.getGeneratedColumns());
        for (int i = 0; i < exprHandle.arity(); ++i) {
            if (exprHandle.isItemChild(i)) {
                final OptItemProperty childProperty = exprHandle.getChildItemProperty(i);
                usedColumns.include(childProperty.getUsedColumns());
                generatedColumns.include(childProperty.getGeneratedColumns());
            } else {
                Preconditions.checkArgument(item.isSubquery());
                usedColumns.include(exprHandle.getChildLogicalProperty(i).getOuterColumns());
            }
        }

        setDistinctAggInfo(item, exprHandle);
    }

    private void setDistinctAggInfo(OptItem item, OptExpressionHandle exprHandle) {
        if (item instanceof OptItemFunctionCall) {
            final OptItemFunctionCall functionCall = (OptItemFunctionCall) item;
            isDistinctAgg = functionCall.getFunction().isDistinct();
            isDisintctAggMultiParam = functionCall.getFunction().getFn().getArgs().length > 0 ? true : false;
        } else if (item instanceof OptItemProjectElement) {
            isDistinctAgg = exprHandle.getChildItemProperty(0).isDistinctAgg;
            isDisintctAggMultiParam = exprHandle.getChildItemProperty(0).isDisintctAggMultiParam;
        } else if (item instanceof OptItemProjectList) {
            for (int i = 0; i < exprHandle.arity(); i++) {
                final OptItemProperty childProperty = exprHandle.getChildItemProperty(i);
                if (childProperty.isDistinctAgg) {
                    distinctAggCount++;
                }
                if (childProperty.isDisintctAggMultiParam) {
                    isDisintctAggMultiParam = true;
                }
            }

            if (distinctAggCount > 1) {
                isDistinctAgg = true;
                isMultiAggDistinct = true;
            } else if (distinctAggCount == 1) {
                isDistinctAgg = true;
            } else {
                Preconditions.checkArgument(distinctAggCount == 0);
            }
        } else {

        }
    }
}
