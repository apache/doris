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
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import java.util.Objects;

/**
 * VarBinary type in Nereids.
 */
public class VarBinaryType extends PrimitiveType {

    public static final int MAX_VARBINARY_LENGTH = ScalarType.MAX_VARCHAR_LENGTH;
    public static final VarBinaryType MAX_VARBINARY_TYPE = new VarBinaryType(MAX_VARBINARY_LENGTH);
    public static final VarBinaryType INSTANCE = new VarBinaryType();
    public int len = -1;

    private VarBinaryType() {
        this.len = MAX_VARBINARY_LENGTH;
    }

    public VarBinaryType(int len) {
        this.len = len;
    }

    public static VarBinaryType createVarBinaryType(int len) {
        if (len == MAX_VARBINARY_LENGTH || len == -1) {
            return MAX_VARBINARY_TYPE;
        }
        return new VarBinaryType(len);
    }

    @Override
    public Type toCatalogDataType() {
        ScalarType catalogDataType = ScalarType.createVarbinaryType(len);
        catalogDataType.setByteSize(len);
        return catalogDataType;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof VarBinaryType;
    }

    @Override
    public String simpleString() {
        return "varbinary";
    }

    @Override
    public DataType defaultConcreteType() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        VarBinaryType that = (VarBinaryType) o;
        return len == that.len;
    }

    @Override
    public int width() {
        return len;
    }

    @Override
    public String toSql() {
        if (len == -1) {
            return "VARBINARY(" + MAX_VARBINARY_LENGTH + ")";
        }
        return "VARBINARY(" + len + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), len);
    }

    public boolean isWildcardVarBinary() {
        return len == -1 || len == MAX_VARBINARY_LENGTH;
    }
}
