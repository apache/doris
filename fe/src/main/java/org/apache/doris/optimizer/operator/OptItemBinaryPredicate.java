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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.catalog.Type;
import org.apache.doris.optimizer.OptUtils;

public class OptItemBinaryPredicate extends OptItem {

    private BinaryPredicate.Operator op;

    public OptItemBinaryPredicate(BinaryPredicate.Operator op) {
        super(OptOperatorType.OP_ITEM_BINARY_PREDICATE);
        this.op = op;
    }

    @Override
    public Type getReturnType() {
        return Type.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), op.hashCode());
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptItemBinaryPredicate rhs = (OptItemBinaryPredicate) object;
        return op == rhs.op;
    }

    @Override
    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ItemBinaryPredicate(op=").append(op).append(")");
        return sb.toString();
    }

    public BinaryPredicate.Operator getOp() { return op; }
}
