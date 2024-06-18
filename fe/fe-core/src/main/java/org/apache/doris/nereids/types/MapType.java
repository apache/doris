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
import org.apache.doris.nereids.annotation.Developing;

import java.util.Objects;

/**
 * Struct type in Nereids.
 */
@Developing
public class MapType extends DataType implements ComplexDataType {

    public static final MapType SYSTEM_DEFAULT = new MapType();

    public static final int WIDTH = 24;

    private final DataType keyType;
    private final DataType valueType;

    private MapType() {
        keyType = NullType.INSTANCE;
        valueType = NullType.INSTANCE;
    }

    private MapType(DataType keyType, DataType valueType) {
        this.keyType = Objects.requireNonNull(keyType, "key type should not be null");
        this.valueType = Objects.requireNonNull(valueType, "value type should not be null");
    }

    public static MapType of(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    @Override
    public DataType conversion() {
        return MapType.of(keyType.conversion(), valueType.conversion());
    }

    @Override
    public Type toCatalogDataType() {
        return new org.apache.doris.catalog.MapType(keyType.toCatalogDataType(), valueType.toCatalogDataType());
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof MapType;
    }

    @Override
    public String simpleString() {
        return "map";
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
        MapType mapType = (MapType) o;
        return Objects.equals(keyType, mapType.keyType) && Objects.equals(valueType, mapType.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "MAP<" + keyType.toSql() + "," + valueType.toSql() + ">";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
