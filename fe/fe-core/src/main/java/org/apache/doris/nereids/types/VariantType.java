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
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import java.util.Objects;

/**
 * Variant type in Nereids.
 * Why Variant is not complex type? Since it's nested structure is not pre-defined, then using
 * primitive type will be easy to handle meta info in FE.
 */
@Developing
public class VariantType extends PrimitiveType {

    public static final VariantType INSTANCE = new VariantType();

    public static final int WIDTH = 24;

    @Override
    public Type toCatalogDataType() {
        return Type.VARIANT;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof VariantType;
    }

    @Override
    public String simpleString() {
        return "variant";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "VARIANT";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
