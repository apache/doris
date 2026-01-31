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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ScalarFunction 'map_concat'
 */
public class MapConcat extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    /**
     * constructor with more than 0 arguments.
     */
    public MapConcat(Expression... varArgs) {
        super("map_concat", varArgs);
    }

    /**
     * private constructor
     */
    private MapConcat(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapConcat withChildren(List<Expression> children) {
        return new MapConcat(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapConcat(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0) {
            return ImmutableList.of(FunctionSignature.ret(MapType.SYSTEM_DEFAULT).args());
        }

        List<Expression> children = children();

        List<DataType> keyTypes = new ArrayList<>();
        List<DataType> valueTypes = new ArrayList<>();

        for (Expression child : children) {
            DataType argType = child.getDataType();
            if (argType instanceof MapType) {
                MapType mapType = (MapType) argType;
                keyTypes.add(mapType.getKeyType());
                valueTypes.add(mapType.getValueType());
            } else if (!(argType instanceof NullType)) {
                throw new AnalysisException("mapconcat function cannot process"
                        + "non-map and non-null child elements. "
                        + "Invalid SQL: " + toSql());
            }
        }
        Optional<DataType> commonKeyType = TypeCoercionUtils.findWiderCommonType(
                keyTypes, true, true);
        Optional<DataType> commonValueType = TypeCoercionUtils.findWiderCommonType(
                valueTypes, true, true);

        if (!commonKeyType.isPresent()) {
            throw new AnalysisException("mapconcat cannot find the common key type of " + toSql());
        }
        if (!commonValueType.isPresent()) {
            throw new AnalysisException("mapconcat cannot find the common value type of " + toSql());
        }

        DataType retMapType = MapType.of(commonKeyType.get(), commonValueType.get());
        ImmutableList.Builder<DataType> retArgTypes = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            DataType argType = children.get(i).getDataType();
            if (argType instanceof MapType) {
                retArgTypes.add(retMapType);
            } else {
                retArgTypes.add(argType);
            }
        }

        return ImmutableList.of(FunctionSignature.of(retMapType, retArgTypes.build()));
    }
}
