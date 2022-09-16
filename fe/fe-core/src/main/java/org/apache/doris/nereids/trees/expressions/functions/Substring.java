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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * substring function.
 */
public class Substring extends ScalarFunction implements ImplicitCastInputTypes {

    // used in interface expectedInputTypes to avoid new list in each time it be called
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(
            TypeCollection.CHARACTER_TYPE_COLLECTION,
            IntegerType.INSTANCE,
            IntegerType.INSTANCE
    );

    public Substring(Expression str, Expression pos, Expression len) {
        super("substring", str, pos, len);
    }

    public Substring(Expression str, Expression pos) {
        super("substring", str, pos);
    }

    public Expression getSource() {
        return child(0);
    }

    public Expression getPosition() {
        return child(1);
    }

    public Optional<Expression> getLength() {
        return arity() == 3 ? Optional.of(child(2)) : Optional.empty();
    }

    @Override
    public DataType getDataType() {
        Optional<Expression> length = getLength();
        if (length.isPresent() && length.get() instanceof IntegerLiteral) {
            return VarcharType.createVarcharType(((IntegerLiteral) length.get()).getValue());
        }
        return VarcharType.SYSTEM_DEFAULT;
    }

    @Override
    public boolean nullable() {
        //TODO: to be compatible with BE, we set true here.
        //return children().stream().anyMatch(Expression::nullable);
        return true;
    }

    @Override
    public Substring withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        if (children.size() == 2) {
            return new Substring(children.get(0), children.get(1));
        }
        return new Substring(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }
}
