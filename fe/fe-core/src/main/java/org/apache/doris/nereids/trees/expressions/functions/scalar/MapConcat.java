// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.thrift.BackendService.AsyncProcessor.publish_cluster_state;
import org.apache.doris.nereids.types.MapType;

import com.amazonaws.services.glue.model.Datatype;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.security.cert.PKIXRevocationChecker.Option;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * ScalarFunction 'map_concat'
 */
public class MapConcat extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {
    private static final Logger LOG = LogManager.getLogger(MapConcat.class);

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
        FunctionSignature
            .ret(MapType.of(new AnyDataType(0), new AnyDataType(1)))
            .varArgs(MapType.of(new AnyDataType(0), new AnyDataType(1)))
        );

    /**
     * constructor with more than 0 arguments.
     */
    public MapConcat(Expression arg, Expression... varArgs) {
        super("map_concat", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /**
     * private constructor
     */
    private MapConcat(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapConcat withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        return new MapConcat(getFunctionParams(children));
    }

    @Override
    public DataType getDataType() {
        if (arity() >= 1){
            List<DataType> keyTypes = new ArrayList<>();
            List<DataType> valueTypes = new ArrayList<>();
            for (int i = 0; i < children.size(); i++){
                DataType argType = children.get(i).getDataType();
                if (!(argType instanceof MapType)){
                    return MapType.SYSTEM_DEFAULT;
                }
                MapType mapType = (MapType) argType;
                keyTypes.add(mapType.getKeyType());
                valueTypes.add(mapType.getValueType());
            }

            Optional<DataType> commonKeyType = TypeCoercionUtils.findWiderCommonType(keyTypes, true, true);
            Optional<DataType> commonValueType = TypeCoercionUtils.findWiderCommonType(valueTypes, true, true);
            if (commonKeyType.isPresent() && commonValueType.isPresent()){
                DataType keyType = commonKeyType.get();
                DataType valueType = commonValueType.get();
                return MapType.of(keyType, valueType);
            }
            if (!commonKeyType.isPresent()) {
                throw new AnalysisException("mapconcat cannot find the common key type of " + this.toSql());
            }
            if (!commonValueType.isPresent()) {
                throw new AnalysisException("mapconcat cannot find the common value type of " + this.toSql());
            }
        }
        throw new RuntimeException("unreachable");
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapConcat(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0){
            return SIGNATURES;
        } else {
            List<FunctionSignature> signatures = ImmutableList.of(
                FunctionSignature.of(getDataType(), 
                children.stream()
                    .map(ExpressionTrait::getDataType)
                    .collect(ImmutableList.toImmutableList())
                ));
            return signatures;
        }
    }
}
