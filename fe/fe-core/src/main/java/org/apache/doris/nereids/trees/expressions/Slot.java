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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all slot in expression.
 */
public abstract class Slot extends NamedExpression implements LeafExpression {
    // for create view, the start and end position of the sql substring
    // (e.g. "col1", "t1.col1", "db1.t1.col1", "ctl1.db1.t1.col1")
    protected final Optional<Pair<Integer, Integer>> indexInSqlString;

    protected Slot(Optional<Pair<Integer, Integer>> indexInSqlString) {
        super(ImmutableList.of());
        this.indexInSqlString = indexInSqlString;
    }

    @Override
    public Slot toSlot() {
        return this;
    }

    public Slot withNullable(boolean nullable) {
        throw new RuntimeException("Do not implement");
    }

    public Slot withNullableAndDataType(boolean nullable, DataType dataType) {
        throw new RuntimeException("Do not implement");
    }

    public Slot withQualifier(List<String> qualifier) {
        throw new RuntimeException("Do not implement");
    }

    public Slot withName(String name) {
        throw new RuntimeException("Do not implement");
    }

    public Slot withExprId(ExprId exprId) {
        throw new RuntimeException("Do not implement");
    }

    public String getInternalName() {
        throw new RuntimeException("Do not implement");
    }

    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        throw new RuntimeException("Do not implement");
    }

    public Optional<Pair<Integer, Integer>> getIndexInSqlString() {
        return indexInSqlString;
    }
}
