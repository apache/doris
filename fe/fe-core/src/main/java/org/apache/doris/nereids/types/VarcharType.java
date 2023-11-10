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
import org.apache.doris.nereids.types.coercion.CharacterType;

import java.util.Objects;

/**
 * Varchar type in Nereids.
 */
public class VarcharType extends CharacterType {

    public static final VarcharType SYSTEM_DEFAULT = new VarcharType(-1);
    public static final int MAX_VARCHAR_LENGTH = ScalarType.MAX_VARCHAR_LENGTH;

    public VarcharType(int len) {
        super(len);
    }

    @Override
    public int width() {
        return len;
    }

    public static VarcharType createVarcharType(int len) {
        if (len == SYSTEM_DEFAULT.len) {
            return SYSTEM_DEFAULT;
        }
        return new VarcharType(len);
    }

    @Override
    public Type toCatalogDataType() {
        ScalarType catalogDataType = ScalarType.createVarcharType(len);
        catalogDataType.setByteSize(len);
        return catalogDataType;
    }

    @Override
    public String simpleString() {
        return "varchar";
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public String toSql() {
        if (len == -1) {
            return "VARCHAR(*)";
        }
        return "VARCHAR(" + len + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        VarcharType that = (VarcharType) o;
        return len == that.len;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), len);
    }
}
