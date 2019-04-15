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
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefSet;

// Reference to a column
public class OptItemColumnRef extends OptItem {
    private OptColumnRef ref;

    public OptItemColumnRef(OptColumnRef ref) {
        super(OptOperatorType.OP_ITEM_COLUMN_REF);
        this.ref = ref;
    }
    public OptColumnRef getRef() { return ref; }

    public OptColumnRef getColumnRef() { return ref; }

    @Override
    public Type getReturnType() {
        return ref.getType();
    }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), ref.getId());
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptItemColumnRef rhs = (OptItemColumnRef) object;
        return ref.getId() == rhs.ref.getId();
    }

    @Override
    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ItemColumnRef(ref=").append(ref).append(")");
        return sb.toString();
    }

    @Override
    public OptColumnRefSet getUsedColumns() {
        return new OptColumnRefSet(ref.getId());
    }
}
