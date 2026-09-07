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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.NestedProjection;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Navigates paimon's type system for {@link NestedProjection}. Stateless, so one instance serves all. */
final class PaimonNestedTypeSource implements NestedProjection.TypeSource<DataType> {

    static final PaimonNestedTypeSource INSTANCE = new PaimonNestedTypeSource();

    private PaimonNestedTypeSource() {
    }

    @Override
    public NestedProjection.Kind kindOf(DataType type) {
        if (type instanceof RowType) {
            return NestedProjection.Kind.STRUCT;
        }
        if (type instanceof ArrayType) {
            return NestedProjection.Kind.ARRAY;
        }
        if (type instanceof MapType) {
            return NestedProjection.Kind.MAP;
        }
        return NestedProjection.Kind.SCALAR;
    }

    @Override
    public List<String> structFieldNames(DataType type) {
        return ((RowType) type).getFieldNames();
    }

    @Override
    public DataType structFieldType(DataType type, int index) {
        return ((RowType) type).getTypeAt(index);
    }

    @Override
    public DataType arrayElementType(DataType type) {
        return ((ArrayType) type).getElementType();
    }

    @Override
    public DataType mapKeyType(DataType type) {
        return ((MapType) type).getKeyType();
    }

    @Override
    public DataType mapValueType(DataType type) {
        return ((MapType) type).getValueType();
    }

    @Override
    public String describe(DataType type) {
        return type.asSQLString();
    }
}
