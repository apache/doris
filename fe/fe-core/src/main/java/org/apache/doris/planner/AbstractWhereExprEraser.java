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
import com.google.common.collect.Range;

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

/*
 *  e.g.
 *  假设一个表按照a、b、c、d列进行Range分区(List分区也类似)。假设以下分区被筛选出来：
 *  [(1,2,3,5),(1,2,4,5)), [(1,2,4,5),(1,2,5,5)), [(1,2,5,5),(1,2,6,5))
 *  执行抽象方法doExpand后，range成为能覆盖它们的最小范围，即：[(1,2,3,5),(1,2,6,5))
 *  +----------------------+-----+-----+-----+-----+
 *  |   columnIndex        |  0  |  1  |  2  |  3  |
 *  +----------------------+-----+-----+-----+-----+
 *  |   columnName         |  a  |  b  |  c  |  d  |
 *  +----------------------+-----+-----+-----+-----+
 *  |   lowerBound(closed) |  1  |  2  |  3  |  5  |
 *  +----------------------+-----+-----+-----+-----+
 *  |   upperBound(open)   |  1  |  2  |  6  |  5  |
 *  +----------------------+-----+-----+-----+-----+
 *
 *  下面穷举一下range里面的值：
 *  (1,2,3,5)、(1,2,3,6)...(1,2,3,+∞)、(1,2,4,-∞)...(1,2,4,+∞)...(1,2,6,-∞)...(1,2,6,4)
 *
 *  用户如果填写的是where a = 1, 那么 a=1 是可以被擦除的了；可以看出range内所有的元素，a恒等于1；
 *  同理，填写a>=1，b=1，c>=3，c<=6，也能擦除，这个看表很容易看出来。但是，在这个例子中，以下几种特殊情况不能直接查表：
 *
 *    d=5：由于c列对应了一个区间而并非单点，因此被选出的区间内或许存在 (1,2,4,any-number) 这类型的列；
 *    但是，根据查询者的要求，他只需要筛选出(1,2,3,5)、(1,2,4,5)、(1,2,5,5)的数据，擦除d=5不合法
 *    而b列及其左边，均为单点，因此不会出现类似情况。为了解决这类问题，应当引入一个变量，记录本用例`b`列的下标
 *
 *    c<6：虽然upperBound是open、开区间，c列对应的也是6，但由于c并非最后一列，因此区间内也会存在(1,2,6,-∞)~(1,2,6,4)的数据
 *    因此c所处的列可以视为区间[3, 6]，即上界也是闭区间，c<6无法完全覆盖。
 *
 *  针对第一种情况，在本用例中，求出maxEqIndex为1，因为下标1之后的是第2列`c`，对应的lowerBound值和upperBound已经不相等了。
 *  因此`d`列(第3列)出现了取值可以为无穷的情况。这种情况下，任何与d相关的表达式都无法擦除
 *
 *  针对第二种情况，当查询者输入的开区间边界恰好等于range中对应列的边界时(本用例中查询者填写了c<6；分区中c列范围是3~6，上界(1,2,6,5)属性是开区间)
 *  必须要此对应列处于最后一列时才允许擦除。本用例d才是最后一列因此不能擦除
 *
 */
public abstract class AbstractWhereExprEraser {
    private static final Logger LOG = LogManager.getLogger(AbstractWhereExprEraser.class);
    protected Range<PartitionKey> range;
    private int maxEqIndex = -1;

    public void initial(List<PartitionItem> partitionItems) throws AnalysisException {
        this.doExpand(partitionItems);
        maxEqIndex = calculateMaxEqIndex();
    }

    private int calculateMaxEqIndex() {
        if(range == null) {
            return -1;
        }
        int size = range.lowerEndpoint().size();
        Preconditions.checkState(size == range.upperEndpoint().size(), "The size of two PartitionKey is not equal.");
        int result = -1;
        for (int index = 0; index < size; index++) {
            LiteralExpr lower = range.lowerEndpoint().getKeyByIndex(index);
            LiteralExpr upper = range.upperEndpoint().getKeyByIndex(index);
            int ret = lower.compareLiteral(upper);
            Preconditions.checkState(ret <= 0, "The lowerEndpoint is larger than the upperEndpoint");
            if (ret < 0) {
                break;
            } else {
                result = index;
            }
        }
        return result;
    }

    /**
     * Make {@linkplain AbstractWhereExprEraser#range} the minimal range
     * which {@linkplain Range#encloses(Range) encloses} all ranges in this list.
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
        if (index > maxEqIndex + 1 || range == null || index >= range.lowerEndpoint().size()) {
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
                return range.lowerEndpoint().getKeyByIndex(index).compareLiteral(literal) >=0
                    && range.upperEndpoint().getKeyByIndex(index).compareLiteral(literal) <= 0;
            case LE:
                return range.upperEndpoint().getKeyByIndex(index).compareLiteral(literal) <= 0;
            case LT:
                return range.upperEndpoint().getKeyByIndex(index).compareLiteral(literal) < 0 ||
                    (index == range.upperEndpoint().size() - 1 && range.upperBoundType() == BoundType.OPEN
                        && range.upperEndpoint().getKeyByIndex(index).compareLiteral(literal) == 0);
            case GE:
                return range.lowerEndpoint().getKeyByIndex(index).compareLiteral(literal) >= 0;
            case GT:
                return range.lowerEndpoint().getKeyByIndex(index).compareLiteral(literal) > 0 ||
                    (index == range.lowerEndpoint().size() - 1 && range.lowerBoundType() == BoundType.OPEN
                        && range.lowerEndpoint().getKeyByIndex(index).compareLiteral(literal) == 0);
            default:
                break;
        }
        return false;
    }
}
