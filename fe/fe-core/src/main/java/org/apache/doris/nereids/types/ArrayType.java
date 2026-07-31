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
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;

import java.util.Objects;

/**
 * Array type in Nereids.
 *
 * <p>Note: Array elements are always nullable in Doris. The NOT NULL constraint on array elements
 * (e.g., ARRAY&lt;INT NOT NULL&gt;) is not supported. This simplification aligns with the actual
 * behavior where both FE planning and BE execution always treat array elements as nullable.</p>
 */
public class ArrayType extends DataType implements ComplexDataType, NestedColumnPrunable {

    public static final ArrayType SYSTEM_DEFAULT = new ArrayType(NullType.INSTANCE);

    public static final int WIDTH = 64;

    private final DataType itemType;

    private ArrayType(DataType itemType) {
        this.itemType = Objects.requireNonNull(itemType, "itemType can not be null");
    }

    public static ArrayType of(DataType itemType) {
        if (itemType.equals(NullType.INSTANCE)) {
            return SYSTEM_DEFAULT;
        }
        return new ArrayType(itemType);
    }

    @Override
    public DataType conversion() {
        return new ArrayType(itemType.conversion());
    }

    public DataType getItemType() {
        return itemType;
    }

    @Override
    public boolean isInjectiveCastTo(DataType target) {
        if (target instanceof ArrayType) {
            return itemType.isInjectiveCastTo(((ArrayType) target).itemType);
        }
        return target instanceof CharacterType;
    }

    @Override
    public Type toCatalogDataType() {
        // Catalog ArrayType defaults containsNull to true via single-arg constructor
        return new org.apache.doris.catalog.ArrayType(itemType.toCatalogDataType());
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
