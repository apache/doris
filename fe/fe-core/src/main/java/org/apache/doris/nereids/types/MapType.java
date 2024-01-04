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
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

/**
 * Struct type in Nereids.
 */
@Developing
public class MapType extends DataType {

    public static final MapType INSTANCE = new MapType();

    public static final MapType SYSTEM_DEFAULT = new MapType();

    public static final int WIDTH = 24;

    private MapType() {
    }

    @Override
    public Type toCatalogDataType() {
        return Type.MAP;
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof MapType;
    }

    @Override
    public String simpleString() {
        return "map";
    }

    @Override
    public DataType defaultConcreteType() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MapType;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "MAP";
    }
}
