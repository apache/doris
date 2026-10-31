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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'hll_from_binary'.
 *
 * Build an HLL from the serialized bytes of a Doris HLL. This is the byte-level
 * counterpart of hll_from_base64: the input VARBINARY holds the raw serialized
 * bytes (e.g. a hive BINARY column read through a catalog with
 * 'enable.mapping.varbinary' = 'true', or the output of hll_to_binary), not the
 * Base64 text form that a TEXTFILE binary column stores.
 */
public class HllFromBinary extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(HllType.INSTANCE).args(VarBinaryType.MAX_VARBINARY_TYPE)
    );

    /**
     * constructor with 1 argument.
     */
    public HllFromBinary(Expression arg) {
        super("hll_from_binary", arg);
    }

    /** constructor for withChildren and reuse signature */
    private HllFromBinary(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public HllFromBinary withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new HllFromBinary(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitHllFromBinary(this, context);
    }
}
