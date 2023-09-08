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

/**
 * Json type in Nereids.
 */
@Developing
public class JsonType extends DataType {

    public static final JsonType INSTANCE = new JsonType();

    public static final int WIDTH = 16;

    private JsonType() {
    }

    @Override
    public Type toCatalogDataType() {
        return Type.JSONB;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof JsonType;
    }

    @Override
    public String simpleString() {
        return "json";
    }

    @Override
    public DataType defaultConcreteType() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JsonType;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "JSON";
    }
}
