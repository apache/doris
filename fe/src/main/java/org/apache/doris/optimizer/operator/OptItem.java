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

package org.apache.doris.optimizer.operator;

import org.apache.doris.catalog.Type;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptItemProperty;
import org.apache.doris.optimizer.base.OptProperty;

public abstract class OptItem extends OptOperator {
    private static OptColumnRefSet EMPTY_COLUMNS = new OptColumnRefSet();
//    protected OptColumnRefSet usedColumns;
//    protected OptColumnRefSet generatedColumns;

    protected OptItem(OptOperatorType type) {
        super(type);
    }

    @Override
    public boolean isItem() { return true; }

    @Override
    public OptProperty createProperty() {
        return new OptItemProperty();
    }

    // Scalar return value's type, every scalar has a return value,
    // such as compare function would return a boolean type result
    public abstract Type getReturnType();

    public boolean isConstant() { return false; }
    public boolean isAlwaysTrue() { return false; }
    public OptColumnRefSet getUsedColumns() { return EMPTY_COLUMNS; }
    public OptColumnRefSet getGeneratedColumns() { return EMPTY_COLUMNS; }
}
