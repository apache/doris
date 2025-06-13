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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ScalarFunction;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.FunctionSignature;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * regexp_position(haystack, pattern[, start_pos]) -> position of first match (1-based), -1 if not found.
 */
public class RegexpPosition extends ScalarFunction implements ExplicitlyCastableSignature {

    public RegexpPosition(Expression... arguments) {
        super("regexp_position", arguments);
    }

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
        // 两参数版本：从头开始查找
        FunctionSignature.ret(BigIntType.INSTANCE)
                         .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
        // 三参数版本：指定起始位置（BigInt）
        FunctionSignature.ret(BigIntType.INSTANCE)
                         .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE)
    );

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public RegexpPosition withChildren(List<Expression> children) {
        return new RegexpPosition(ExpressionUtils.checkArity(children, 2, 3)
            .toArray(new Expression[0]));
    }
}
