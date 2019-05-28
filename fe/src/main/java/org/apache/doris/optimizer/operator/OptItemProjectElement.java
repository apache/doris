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

public class OptItemProjectElement extends OptItem {
    private OptColumnRef column;

    public OptItemProjectElement(OptColumnRef ref) {
        super(OptOperatorType.OP_ITEM_PROJECT_ELEMENT);
        this.column = ref;
    }

    @Override
    public Type getReturnType() {
        return null;
    }

    @Override
    public OptColumnRefSet getGeneratedColumns() {
        final OptColumnRefSet columnRefSet = new OptColumnRefSet();
        columnRefSet.include(column);
        return columnRefSet;
    }

    public OptColumnRef getColumn() {
        return column;
    }
}
