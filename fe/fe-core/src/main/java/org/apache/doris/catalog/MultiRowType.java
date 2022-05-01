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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Describes a multi-row type in sub-query.
 */
public class MultiRowType extends Type {
    private final Type itemType;

    public MultiRowType(Type itemType) {
        this.itemType = itemType;
    }

    public Type getItemType() {
        return itemType;
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "ARRAY<...>";
        }
        return String.format("ARRAY<%s>", itemType.toSql(depth + 1));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MultiRowType)) {
            return false;
        }
        MultiRowType otherMultiRowType = (MultiRowType) other;
        return otherMultiRowType.itemType.equals(itemType);
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(itemType);
        node.setType(TTypeNodeType.ARRAY);
        itemType.toThrift(container);
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        if (!itemType.isStructType()) {
            return leftPadding + toSql();
        }
        // Pass in the padding to make sure nested fields are aligned properly,
        // even if we then strip the top-level padding.
        String structStr = itemType.prettyPrint(lpad);
        structStr = structStr.substring(lpad);
        return String.format("%sARRAY<%s>", leftPadding, structStr);
    }
}
