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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {

    @SerializedName(value = "itemType")
    private Type itemType;

    public ArrayType() {
        this.itemType = NULL;
    }

    public ArrayType(Type itemType) {
        this.itemType = itemType;
    }

    public void setItemType(Type itemType) {
        this.itemType = itemType;
    }

    public Type getItemType() {
        return itemType;
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.ARRAY;
    }

    @Override
    public boolean matchesType(Type t) {
        if (equals(t)) {
            return true;
        }

        if (!t.isArrayType()) {
            return false;
        }

        if (itemType.isNull()) {
            return true;
        }

        return itemType.matchesType(((ArrayType) t).itemType);
    }

    public static ArrayType create() {
        return new ArrayType();
    }

    public static ArrayType create(Type type) {
        return new ArrayType(type);
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
        if (!(other instanceof ArrayType)) {
            return false;
        }
        ArrayType otherArrayType = (ArrayType) other;
        return otherArrayType.itemType.equals(itemType);
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

    @Override
    public boolean isSupported() {
        if (!Config.enable_complex_type_support) {
            return false;
        }

        if (itemType.isNull()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return toSql(0);
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = PrimitiveType.ARRAY.toThrift();
        return thrift;
    }

    @Override
    public boolean isFixedLengthType() {
        return false;
    }

    @Override
    public boolean supportsTablePartitioning() {
        if (!isSupported() || isComplexType()) {
            return false;
        }
        return true;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.ARRAY.getSlotSize();
    }
}
