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

// this predicate may have many children,
// OptItemInPredicate
// |--- child0
// |--- child1
// |--- child2
// ...
// which indicates 'child0 in (child1, child1, ...)'
public class OptItemInPredicate extends OptItem {
    boolean isNotIn;

    public OptItemInPredicate(boolean isNotIn) {
        super(OptOperatorType.OP_ITEM_IN_PREDICATE);
        this.isNotIn = isNotIn;
    }
    public boolean isNotIn() { return isNotIn; }

    @Override
    public Type getReturnType() {
        return Type.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), isNotIn);
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptItemInPredicate rhs = (OptItemInPredicate) object;
        return isNotIn == rhs.isNotIn;
    }

    @Override
    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ItemInPredicate(isNotIn=").append(isNotIn).append(")");
        return sb.toString();
    }
}
