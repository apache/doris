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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.UnaryExpression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FractionalType;
import org.apache.doris.nereids.types.IntegralType;

import com.google.common.base.Preconditions;

import java.util.List;

/** sum agg function. */
public class Sum extends AggregateFunction implements UnaryExpression<Expression> {

    public Sum(Expression child) {
        super("sum", child);
    }

    @Override
    public DataType getDataType() {
        DataType dataType = child().getDataType();
        if (dataType instanceof IntegralType) {
            return BigIntType.INSTANCE;
        } else if (dataType instanceof FractionalType) {
            // TODO: precision + 10
            return DoubleType.INSTANCE;
        } else {
            throw new IllegalStateException("Unsupported sum type: " + dataType);
        }
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Sum(children.get(0));
    }

    @Override
    public DataType getIntermediateType() {
        return getDataType();
    }
}
