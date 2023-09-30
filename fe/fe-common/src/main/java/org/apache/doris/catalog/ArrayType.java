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

import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Objects;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {

    public static final int MAX_NESTED_DEPTH = 9;

    @SerializedName(value = "itemType")
    private Type itemType;

    @SerializedName(value = "containsNull")
    private boolean containsNull;

    public ArrayType() {
        itemType = NULL;
        containsNull = true;
    }

    public ArrayType(Type itemType) {
        this(itemType, true);
    }

    public ArrayType(Type itemType, boolean containsNull) {
        this.itemType = itemType;
        this.containsNull = containsNull;
    }

    public Type getItemType() {
        return itemType;
    }

    public boolean getContainsNull() {
        return containsNull;
    }

    public void setContainsNull(boolean containsNull) {
        this.containsNull = containsNull;
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

        if (t.isAnyType()) {
            return t.matchesType(this);
        }

        if (!t.isArrayType()) {
            return false;
        }

        // Array(Null) is a virtual Array type, can match any Array(...) type
        if (itemType.isNull() || ((ArrayType) t).getItemType().isNull()) {
            return true;
        }

        return itemType.matchesType(((ArrayType) t).itemType)
                && (((ArrayType) t).containsNull || !containsNull);
    }

    @Override
    public boolean hasTemplateType() {
        return itemType.hasTemplateType();
    }

    @Override
    public Type specializeTemplateType(Type specificType, Map<String, Type> specializedTypeMap,
                                       boolean useSpecializedType) throws TypeException {
        ArrayType specificArrayType = null;
        if (specificType instanceof ArrayType) {
            specificArrayType = (ArrayType) specificType;
        } else if (!useSpecializedType) {
            throw new TypeException(specificType + " is not ArrayType");
        }

        Type newItemType = itemType;
        if (itemType.hasTemplateType()) {
            newItemType = itemType.specializeTemplateType(
                specificArrayType != null ? specificArrayType.itemType : specificType,
                specializedTypeMap, useSpecializedType);
        }

        return new ArrayType(newItemType);
    }

    public static ArrayType create() {
        return new ArrayType();
    }

    public static ArrayType create(Type type, boolean containsNull) {
        return new ArrayType(type, containsNull);
    }

    @Override
    public String toSql(int depth) {
        if (!containsNull) {
            return "array<not_null(" + itemType.toSql(depth + 1) + ")>";
        } else {
            return "array<" + itemType.toSql(depth + 1) + ">";
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemType, containsNull);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ArrayType)) {
            return false;
        }
        ArrayType otherArrayType = (ArrayType) other;
        return otherArrayType.itemType.equals(itemType) && otherArrayType.containsNull == containsNull;
    }

    public static boolean canCastTo(ArrayType type, ArrayType targetType) {
        if (!targetType.containsNull && type.containsNull) {
            return false;
        }
        if (targetType.getItemType().isStringType() && type.getItemType().isStringType()) {
            return true;
        }
        return Type.canCastTo(type.getItemType(), targetType.getItemType());
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(itemType);
        node.setType(TTypeNodeType.ARRAY);
        node.setContainsNull(containsNull);
        node.setContainsNulls(Lists.newArrayList(containsNull));
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
        String structStr = itemType.prettyPrint(lpad).substring(lpad);
        return String.format("%sARRAY<%s>", leftPadding, structStr);
    }

    @Override
    public boolean isSupported() {
        return !itemType.isNull();
    }

    @Override
    public boolean supportSubType(Type subType) {
        for (Type supportedType : getArraySubTypes()) {
            if (subType.getPrimitiveType() == supportedType.getPrimitiveType()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("ARRAY<%s>", itemType.toString()).toUpperCase();
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
        return false;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.ARRAY.getSlotSize();
    }
}
