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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.ComplexDataType;

import java.util.Objects;

/**
 * Array type in Nereids.
 */
public class ArrayType extends DataType implements ComplexDataType {

    public static final ArrayType SYSTEM_DEFAULT = new ArrayType(NullType.INSTANCE, true);

    public static final int WIDTH = 64;

    private final DataType itemType;
    private final boolean containsNull;

    private ArrayType(DataType itemType, boolean containsNull) {
        this.itemType = Objects.requireNonNull(itemType, "itemType can not be null");
        this.containsNull = containsNull;
    }

    public static ArrayType of(DataType itemType) {
        return of(itemType, true);
    }

    public static ArrayType of(DataType itemType, boolean containsNull) {
        if (itemType.equals(NullType.INSTANCE)) {
            return SYSTEM_DEFAULT;
        }
        return new ArrayType(itemType, containsNull);
    }

    @Override
    public DataType conversion() {
        return new ArrayType(itemType.conversion(), containsNull);
    }

    public DataType getItemType() {
        return itemType;
    }

    public boolean containsNull() {
        return containsNull;
    }

    @Override
    public Type toCatalogDataType() {
        return new org.apache.doris.catalog.ArrayType(itemType.toCatalogDataType(), containsNull);
    }

    @Override
    public String simpleString() {
        return "array";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ArrayType arrayType = (ArrayType) o;
        return Objects.equals(itemType, arrayType.itemType)
                && Objects.equals(containsNull, arrayType.containsNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), itemType);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "ARRAY<" + itemType.toSql() + ">";
    }
}
