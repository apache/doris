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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.junit.jupiter.api.Assertions;

public class ExpressionParseChecker extends ParseChecker {
    private final Supplier<Expression> parsedSupplier;

    public ExpressionParseChecker(String sql) {
        super(sql);
        this.parsedSupplier = Suppliers.memoize(() -> PARSER.parseExpression(sql));
    }

    public ExpressionParseChecker assertEquals(Expression expected) {
        Assertions.assertEquals(expected, parsedSupplier.get());
        return this;
    }

    public <T extends Throwable> ExceptionChecker assertThrows(Class<T> expectedType) {
        return new ExceptionChecker(Assertions.assertThrows(expectedType, parsedSupplier::get));
    }

    public <T extends Throwable> ExceptionChecker assertThrowsExactly(Class<T> expectedType) {
        return new ExceptionChecker(Assertions.assertThrowsExactly(expectedType, parsedSupplier::get));
    }
}
