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

import java.util.Objects;

/**
 * Describes an ARRAY type.
 *
 * <p>Array elements are always nullable in Doris. The {@code containsNull} field is retained
 * only for backward compatibility with serialized metadata (Gson). It is always treated as
 * {@code true} at runtime. The two-argument constructor is kept but deprecated; prefer the
 * single-argument constructor {@link #ArrayType(Type)} which defaults to nullable elements.</p>
 */
public class ArrayType extends Type {

    public static final int MAX_NESTED_DEPTH = 9;

    @SerializedName(value = "itemType")
    private final Type itemType;

    // Retained for Gson deserialization compatibility with older metadata.
    // Always treated as true at runtime — array elements are always nullable.
    @SerializedName(value = "containsNull")
    private final boolean containsNull;

    public ArrayType() {
        itemType = NULL;
        containsNull = true;
    }

    public ArrayType(Type itemType) {
        this.itemType = itemType;
        this.containsNull = true;
    }

    /**
     * @deprecated Array elements are always nullable. Use {@link #ArrayType(Type)} instead.
     *             This constructor is retained only for call-site compatibility during transition.
     */
    @Deprecated
    public ArrayType(Type itemType, boolean containsNull) {
        this.itemType = itemType;
        // Ignore the parameter — always true
        this.containsNull = true;
    }

    public Type getItemType() {
        return itemType;
    }

    /**
     * Always returns {@code true}. Array elements are always nullable in Doris.
     */
    public boolean getContainsNull() {
        // Always true — array elements are always nullable
        return true;
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

        // containsNull is always true, no need to compare
        return itemType.matchesType(((ArrayType) t).itemType);
    }

    public static ArrayType create() {
        return new ArrayType();
    }

    public static ArrayType create(Type type) {
        return new ArrayType(type);
    }

    /**
     * @deprecated Array elements are always nullable. Use {@link #create(Type)} instead.
     */
    @Deprecated
    public static ArrayType create(Type type, boolean containsNull) {
        return new ArrayType(type);
    }

    @Override
    public String toSql(int depth) {
        // Array elements are always nullable, no "not null" suffix needed
        return "array<" + itemType.toSql(depth + 1) + ">";
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemType);
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
        // Array elements are always nullable
        node.setContainsNull(true);
        node.setContainsNulls(Lists.newArrayList(true));
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
        return itemType.isSupported() && !itemType.isNull();
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
        return String.format("array<%s>", itemType.toString());
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = PrimitiveType.ARRAY.toThrift();
        return thrift;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.ARRAY.getSlotSize();
    }
}
