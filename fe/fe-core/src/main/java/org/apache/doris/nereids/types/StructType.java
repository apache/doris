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
import org.apache.doris.nereids.analyzer.ComplexDataType;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Struct type in Nereids.
 */
@Developing
public class StructType extends DataType implements ComplexDataType {

    public static final StructType SYSTEM_DEFAULT = new StructType();

    public static final int WIDTH = 24;

    private final List<StructField> fields;
    private final Supplier<Map<String, StructField>> nameToFields;

    private StructType() {
        nameToFields = Suppliers.memoize(ImmutableMap::of);
        fields = ImmutableList.of();
    }

    /**
     * construct struct type.
     */
    public StructType(List<StructField> fields) {
        this.fields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.nameToFields = Suppliers.memoize(() -> this.fields.stream().collect(ImmutableMap.toImmutableMap(
                StructField::getName, f -> f, (f1, f2) -> {
                    throw new AnalysisException("The name of the struct field cannot be repeated."
                            + " same name fields are " + f1 + " and " + f2);
                })));
    }

    public List<StructField> getFields() {
        return fields;
    }

    public Map<String, StructField> getNameToFields() {
        return nameToFields.get();
    }

    @Override
    public DataType conversion() {
        return new StructType(fields.stream().map(StructField::conversion).collect(Collectors.toList()));
    }

    @Override
    public Type toCatalogDataType() {
        return new org.apache.doris.catalog.StructType(fields.stream()
                .map(StructField::toCatalogDataType)
                .collect(Collectors.toCollection(ArrayList::new)));
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof StructType;
    }

    @Override
    public String simpleString() {
        return "struct";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StructType that = (StructType) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return "STRUCT<" + fields.stream().map(StructField::toSql).collect(Collectors.joining(",")) + ">";
    }

    @Override
    public String toString() {
        return "STRUCT<" + fields.stream().map(StructField::toString).collect(Collectors.joining(",")) + ">";
    }
}
