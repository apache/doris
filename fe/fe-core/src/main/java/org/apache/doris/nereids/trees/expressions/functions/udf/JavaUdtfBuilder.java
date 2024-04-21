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

package org.apache.doris.nereids.trees.expressions.functions.udf;

import org.apache.doris.common.Pair;
import org.apache.doris.common.util.ReflectionUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * function builder for java udtf
 */
public class JavaUdtfBuilder extends UdfBuilder {
    private final JavaUdtf udf;
    private final int arity;
    private final boolean isVarArgs;

    public JavaUdtfBuilder(JavaUdtf udf) {
        this.udf = udf;
        this.isVarArgs = udf.hasVarArguments();
        this.arity = udf.arity();
    }

    @Override
    public List<DataType> getArgTypes() {
        return Suppliers.memoize(() -> udf.getSignatures().get(0).argumentsTypes.stream()
                .map(DataType.class::cast)
                .collect(Collectors.toList())).get();
    }

    @Override
    public Class<? extends BoundFunction> functionClass() {
        return JavaUdtf.class;
    }

    @Override
    public boolean canApply(List<?> arguments) {
        if ((isVarArgs && arity > arguments.size() + 1) || (!isVarArgs && arguments.size() != arity)) {
            return false;
        }
        for (Object argument : arguments) {
            if (!(argument instanceof Expression)) {
                Optional<Class> primitiveType = ReflectionUtils.getPrimitiveType(argument.getClass());
                if (!primitiveType.isPresent() || !Expression.class.isAssignableFrom(primitiveType.get())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Pair<JavaUdtf, JavaUdtf> build(String name, List<?> arguments) {
        List<Expression> exprs = arguments.stream().map(Expression.class::cast).collect(Collectors.toList());
        List<DataType> argTypes = udf.getSignatures().get(0).argumentsTypes;

        List<Expression> processedExprs = Lists.newArrayList();
        for (int i = 0; i < exprs.size(); ++i) {
            processedExprs.add(TypeCoercionUtils.castIfNotSameType(exprs.get(i), argTypes.get(i)));
        }
        return Pair.ofSame(udf.withChildren(processedExprs));
    }
}
