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
 * Describes a MAP type. MAP types have a scalar key and an arbitrarily-typed value.
 */
public class MapType extends Type {

    @SerializedName(value = "keyType")
    private final Type keyType;

    @SerializedName(value = "isKeyContainsNull")
    private final boolean isKeyContainsNull; // Now always true

    @SerializedName(value = "valueType")
    private final Type valueType;

    @SerializedName(value = "isValueContainsNull")
    private final boolean isValueContainsNull; // Now always true

    public MapType() {
        this.keyType = NULL;
        this.isKeyContainsNull = true;
        this.valueType = NULL;
        this.isValueContainsNull = true;
    }

    public MapType(Type keyType, Type valueType) {
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        this.keyType = keyType;
        this.isKeyContainsNull = true;
        this.valueType = valueType;
        this.isValueContainsNull = true;
    }

    public MapType(Type keyType, Type valueType, boolean keyContainsNull, boolean valueContainsNull) {
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        this.keyType = keyType;
        this.isKeyContainsNull = keyContainsNull;
        this.valueType = valueType;
        this.isValueContainsNull = valueContainsNull;
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.MAP;
    }

    public Type getKeyType() {
        return keyType;
    }

    public Boolean getIsKeyContainsNull() {
        return isKeyContainsNull;
    }

    public Boolean getIsValueContainsNull() {
        return isValueContainsNull;
    }

    public Type getValueType() {
        return valueType;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MapType)) {
            return false;
        }
        MapType otherMapType = (MapType) other;
        return otherMapType.keyType.equals(keyType)
                && otherMapType.valueType.equals(valueType);
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "map<...>";
        }
        return String.format("map<%s,%s>",
                keyType.toSql(depth + 1), valueType.toSql(depth + 1));
    }

    @Override
    public boolean matchesType(Type t) {
        if (equals(t)) {
            return true;
        }

        if (t.isAnyType()) {
            return t.matchesType(this);
        }

        if (!t.isMapType()) {
            return false;
        }

        if (((MapType) t).getIsKeyContainsNull() != getIsKeyContainsNull()) {
            return false;
        }
        if (((MapType) t).getIsValueContainsNull() != getIsValueContainsNull()) {
            return false;
        }

        return keyType.matchesType(((MapType) t).keyType)
            && (valueType.matchesType(((MapType) t).valueType));
    }

    @Override
    public String toString() {
        return String.format("map<%s,%s>",
                keyType.toString(), valueType.toString());
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        if (!valueType.isStructType()) {
            return leftPadding + toSql();
        }
        // Pass in the padding to make sure nested fields are aligned properly,
        // even if we then strip the top-level padding.
        String structStr = valueType.prettyPrint(lpad);
        structStr = structStr.substring(lpad);
        return String.format("%sMAP<%s,%s>", leftPadding, keyType.toSql(), structStr);
    }

    @Override
    public boolean supportSubType(Type subType) {
        for (Type supportedType : Type.getMapSubTypes()) {
            if (subType.getPrimitiveType() == supportedType.getPrimitiveType()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.MAP.getSlotSize();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        node.setType(TTypeNodeType.MAP);
        node.setContainsNulls(Lists.newArrayList(isKeyContainsNull, isValueContainsNull));
        keyType.toThrift(container);
        valueType.toThrift(container);
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = PrimitiveType.MAP.toThrift();
        return thrift;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType);
    }

    @Override
    public boolean isSupported() {
        return keyType.isSupported() && !keyType.isNull() && valueType.isSupported() && !valueType.isNull();
    }
}
