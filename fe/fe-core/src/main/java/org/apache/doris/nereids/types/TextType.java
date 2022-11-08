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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import java.util.Objects;

/**
 * Text type in Nereids.
 */
public class TextType extends CharacterType {

    public static final TextType SYSTEM_DEFAULT = new TextType(-1);

    public TextType(int len) {
        super(len);
    }

    public static TextType createVarcharType(int len) {
        return new TextType(len);
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createVarcharType(len);
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof TextType;
    }

    @Override
    public String simpleString() {
        return "text";
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public String toSql() {
        return "TEXT(" + len + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        TextType that = (TextType) o;
        return len == that.len;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), len);
    }
}
