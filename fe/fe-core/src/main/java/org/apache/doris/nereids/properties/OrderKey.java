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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Expression;

/**
 * Represents the order key of a statement.
 */
public class OrderKey {

    private Expression expr;

    // Order is ascending.
    private boolean isAsc;

    private boolean nullFirst;

    /**
     * Constructor of OrderKey.
     *
     * @param nullFirst True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
     */
    public OrderKey(Expression expr, boolean isAsc, boolean nullFirst) {
        this.expr = expr;
        this.isAsc = isAsc;
        this.nullFirst = nullFirst;
    }

    public Expression getExpr() {
        return expr;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public boolean isNullFirst() {
        return nullFirst;
    }

    @Override
    public String toString() {
        return expr.toSql();
    }
}
