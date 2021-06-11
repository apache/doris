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

package org.apache.doris.planner;

import com.google.common.collect.BoundType;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.parquet.Preconditions;

import java.util.List;

public abstract class AbstractAfterPartitionPruningWhereExprEraser extends BoundExpander<PartitionKey> {
    private static final Logger LOG = LogManager.getLogger(AbstractAfterPartitionPruningWhereExprEraser.class);

    private int maxEqIndex = -1;

    public void initial(List<PartitionItem> partitionItems) throws AnalysisException {
        lowerBound = null;
        upperBound = null;
        this.doExpand(partitionItems);
        maxEqIndex = -1;
        if(lowerBound == null || upperBound == null) {
            return;
        }
        int size = lowerBound.size();
        Preconditions.checkState(size == upperBound.size(), "The size of two PartitionKey is not equal.");
        for (int index = 0; index < size; index++) {
            LiteralExpr l = lowerBound.getKeyByIndex(index);
            LiteralExpr u = upperBound.getKeyByIndex(index);
            int ret = l.compareLiteral(u);
            if (ret < 0) {
                break;
            } else if (ret == 0) {
                maxEqIndex = index;
            } else {
                throw new AnalysisException("");
            }
        }
    }

    /**
     * 扩张边界，使lowerBound成为partitionItems的共同下界。
     */
    protected abstract void doExpand(List<PartitionItem> partitionItems);

    public final void eraseConjuncts(List<Expr> conjuncts, List<Column> partitionColumns, TupleDescriptor desc) {
        for (int index = 0; index < partitionColumns.size(); index++) {
            Column column = partitionColumns.get(index);
            SlotDescriptor slotDescriptor = desc.getColumnSlot(column.getName());
            if (slotDescriptor != null) {
                SlotId slotId = slotDescriptor.getId();
                int finalIndex = index;
                conjuncts.removeIf(expr -> expr.isBound(slotId) && expr instanceof BinaryPredicate
                    && shouldErase(finalIndex, slotId, (BinaryPredicate) expr));
            }
        }
    }

    private boolean shouldErase(int index, SlotId slotId, BinaryPredicate binPredicate) {
        if (index > maxEqIndex + 1 || lowerBound == null || index >= lowerBound.size() || upperBound == null) {
            return false;
        }
        Expr slotBinding = binPredicate.getSlotBinding(slotId);
        if (slotBinding == null || !slotBinding.isConstant()) {
            return false;
        }
        if (binPredicate.getOp() == BinaryPredicate.Operator.NE
            || !(slotBinding instanceof LiteralExpr)) {
            return false;
        }
        LiteralExpr literal = (LiteralExpr) slotBinding;
        BinaryPredicate.Operator op = binPredicate.getOp();
        if (!binPredicate.slotIsLeft()) {
            op = op.converse();
        }

        switch (op) {
            case EQ:
                return lowerBound.getKeyByIndex(index).compareLiteral(literal) >=0 && upperBound.getKeyByIndex(index).compareLiteral(literal) <= 0;
            case LE:
                return upperBound.getKeyByIndex(index).compareLiteral(literal) <= 0;
            case LT:
                return upperBound.getKeyByIndex(index).compareLiteral(literal) < 0 ||
                    (index == maxEqIndex + 1 && upperBoundType == BoundType.OPEN && upperBound.getKeyByIndex(index).compareLiteral(literal) == 0);
            case GE:
                return lowerBound.getKeyByIndex(index).compareLiteral(literal) >= 0;
            case GT:
                return lowerBound.getKeyByIndex(index).compareLiteral(literal) > 0 ||
                    (index == maxEqIndex + 1 && lowerBoundType == BoundType.OPEN && lowerBound.getKeyByIndex(index).compareLiteral(literal) == 0);
            default:
                break;
        }
        return false;
    }
}
