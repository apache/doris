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

package org.apache.doris.nereids.types.coercion;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A collection of types that can be used to specify type constraints. The sequence also specifies
 * precedence: an earlier type takes precedence over a latter type.
 * <p />TypeCollection(StringType, NumericType)
 * <p />This means that we prefer StringType over NumericType if it is possible to cast to StringType.
 */
public class TypeCollection implements AbstractDataType {

    public static final TypeCollection CHARACTER_TYPE_COLLECTION
            = new TypeCollection(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);

    private final List<AbstractDataType> types;

    public TypeCollection(List<AbstractDataType> types) {
        Objects.requireNonNull(types);
        Preconditions.checkArgument(!types.isEmpty());
        this.types = ImmutableList.copyOf(types);
    }

    public TypeCollection(AbstractDataType... types) {
        Preconditions.checkArgument(types.length > 0);
        this.types = ImmutableList.copyOf(types);
    }

    public List<AbstractDataType> getTypes() {
        return types;
    }

    @Override
    public DataType defaultConcreteType() {
        return types.get(0).defaultConcreteType();
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return types.stream().anyMatch(t -> t.acceptsType(other));
    }

    @Override
    public Type toCatalogDataType() {
        throw new AnalysisException("TypeCollection can not cast to catalog data type");
    }

    @Override
    public String simpleString() {
        return types.stream().map(AbstractDataType::simpleString).collect(Collectors.joining(" or ", "(", ")"));
    }
}
