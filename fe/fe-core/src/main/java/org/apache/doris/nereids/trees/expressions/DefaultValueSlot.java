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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import java.util.Optional;

/**
 * only use for insert into t values(DEFAULT, ...)
 */
public class DefaultValueSlot extends Slot {

    public DefaultValueSlot() {
        super(Optional.empty());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDefaultValue(this, context);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String getName() throws UnboundException {
        return "$default";
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return NullType.INSTANCE;
    }

    @Override
    public Slot toSlot() throws UnboundException {
        return this;
    }

    @Override
    public String toString() {
        return "DEFAULT_VALUE";
    }

    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return this;
    }
}
