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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Id;
import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TVFProperties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.tablefunction.NumbersTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/** Numbers */
public class Numbers extends TableValuedFunction {
    public Numbers(TVFProperties properties) {
        super("numbers", properties);
    }

    @Override
    public FunctionSignature customSignature(List<DataType> argumentTypes, List<Expression> arguments) {
        return FunctionSignature.of(BigIntType.INSTANCE, (List) argumentTypes);
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return new NumbersTableValuedFunction(arguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build NumbersTableValuedFunction by "
                    + this + ": " + t.getMessage(), t);
        }
    }

    @Override
    public StatsDeriveResult computeStats(List<Slot> slots) {
        Preconditions.checkArgument(slots.size() == 1);
        try {
            NumbersTableValuedFunction catalogFunction = (NumbersTableValuedFunction) getCatalogFunction();
            long rowNum = catalogFunction.getTotalNumbers();

            Map<Id, ColumnStatistic> columnToStatistics = Maps.newHashMap();
            ColumnStatistic columnStat = new ColumnStatistic(rowNum, rowNum, 8, 0, 8, 0, rowNum - 1,
                    1.0 / rowNum, new IntLiteral(0, Type.BIGINT), new IntLiteral(rowNum - 1, Type.BIGINT), false);
            columnToStatistics.put(slots.get(0).getExprId(), columnStat);
            return new StatsDeriveResult(rowNum, columnToStatistics);
        } catch (Exception t) {
            throw new NereidsException(t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNumbers(this, context);
    }

    @Override
    public Numbers withChildren(List<Expression> children) {
        Preconditions.checkArgument(children().size() == 1
                && children().get(0) instanceof TVFProperties);
        return new Numbers((TVFProperties) children.get(0));
    }
}
