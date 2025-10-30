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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.MoreFieldsThread;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**Params*/
public class Params {
    public final Optional<Expression> originExpression;
    public final Operator legacyOperator;
    public final List<Expression> children;
    public final boolean inferred;

    public Params(Expression originExpression, Operator legacyOperator, List<Expression> children, boolean inferred) {
        this.originExpression = MoreFieldsThread.isKeepFunctionSignature()
                ? Optional.ofNullable(originExpression) : Optional.empty();
        this.legacyOperator = legacyOperator;
        this.children = children;
        this.inferred = inferred;
    }

    public Supplier<DataType> getOriginDataType() {
        if (originExpression.isPresent()) {
            return () -> originExpression.get().getDataType();
        } else {
            return null;
        }
    }
}
