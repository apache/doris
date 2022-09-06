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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * weekOfYear function
 */
public class WeekOfYear extends BoundFunction implements UnaryExpression, ImplicitCastInputTypes {

    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(
            new TypeCollection(DateTimeType.INSTANCE)
    );

    public WeekOfYear(Expression child) {
        super("weekofyear", child);
    }

    @Override
    public DataType getDataType() {
        return IntegerType.INSTANCE;
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public WeekOfYear withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new WeekOfYear(children.get(0));
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }

}
