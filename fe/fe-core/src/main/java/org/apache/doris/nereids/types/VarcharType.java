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

import java.util.Objects;

/**
 * Varchar type in Nereids.
 */
public class VarcharType extends DataType {
    private final int len;

    public VarcharType(int len) {
        this.len = len;
    }

    public static VarcharType createVarcharType(int len) {
        return new VarcharType(len);
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createVarcharType(len);
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
