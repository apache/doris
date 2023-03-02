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
public class StructType extends DataType {

    public static final StructType INSTANCE = new StructType();

    public static final int WIDTH = 24;

    private StructType() {
    }

    @Override
    public Type toCatalogDataType() {
        return Type.STRUCT;
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof StructType;
    }

    @Override
    public String simpleString() {
        return "struct";
    }

    @Override
    public DataType defaultConcreteType() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StructType;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "STRUCT";
    }
}
