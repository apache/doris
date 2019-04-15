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
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptItem;

public class OptItemProperty implements OptProperty {
    private OptColumnRefSet usedColumns;
    private OptColumnRefSet definedColumns;

    public OptColumnRefSet getUsedColumns() { return usedColumns; }
    public OptColumnRefSet getDefinedColumns() { return definedColumns; }

    @Override
    public void derive(OptExpressionHandle exprHandle) {
        Preconditions.checkArgument(exprHandle.getOp() instanceof OptItem,
                "op is not item, op=" + exprHandle.getOp());
        OptItem item = (OptItem) exprHandle.getOp();
        usedColumns = item.getUsedColumns();
        for (int i = 0; i < exprHandle.arity(); ++i) {
            // TODO(zc)
            /*
            if (!input.getOp().isItem()) {
                continue;
            }
            OptItemProperty property = (OptItemProperty) input.getProperty();
            // Include input's used columns
            usedColumns.include(property.getUsedColumns());
            */
        }
    }
}
