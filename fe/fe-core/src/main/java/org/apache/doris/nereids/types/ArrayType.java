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

import java.util.Objects;

/**
 * Bitmap type in Nereids.
 */
public class ArrayType extends DataType {

    public static final ArrayType SYSTEM_DEFAULT = new ArrayType(NullType.INSTANCE);

    public static final int WIDTH = 32;

    private final DataType itemType;

    public ArrayType(DataType itemType) {
        this.itemType = Objects.requireNonNull(itemType, "itemType can not be null");
    }

    public static ArrayType of(DataType itemType) {
        if (itemType.equals(NullType.INSTANCE)) {
            return SYSTEM_DEFAULT;
        }
        return new ArrayType(itemType);
    }

    @Override
    public Type toCatalogDataType() {
        return Type.ARRAY;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof ArrayType;
    }

    @Override
    public String simpleString() {
        return "array";
    }

    @Override
    public DataType defaultConcreteType() {
        return SYSTEM_DEFAULT;
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
        return Objects.equals(itemType, arrayType.itemType);
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
