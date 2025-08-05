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
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Variant type in Nereids.
 * Why Variant is not complex type? Since it's nested structure is not pre-defined, then using
 * primitive type will be easy to handle meta info in FE.
 * Also, could predefine some fields of nested columns.
 * Example: VARIANT <`a.b`:INT, a.c:DATETIMEV2>
 *
 */
public class VariantType extends PrimitiveType {

    public static final VariantType INSTANCE = new VariantType(0);

    public static final int WIDTH = 24;

    private final int variantMaxSubcolumnsCount;

    private final boolean enableTypedPathsToSparse;

    private final List<VariantField> predefinedFields;

    // No predefined fields
    public VariantType(int variantMaxSubcolumnsCount) {
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
        this.predefinedFields = Lists.newArrayList();
        this.enableTypedPathsToSparse = false;
    }

    /**
     *   Contains predefined fields like struct
     */
    public VariantType(List<VariantField> fields) {
        this.predefinedFields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
    }

    public VariantType(List<VariantField> fields, int variantMaxSubcolumnsCount, boolean enableTypedPathsToSparse) {
        this.predefinedFields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
        this.enableTypedPathsToSparse = enableTypedPathsToSparse;
    }

    @Override
    public DataType conversion() {
        return new VariantType(predefinedFields.stream().map(VariantField::conversion)
                                .collect(Collectors.toList()), variantMaxSubcolumnsCount, enableTypedPathsToSparse);
    }

    @Override
    public Type toCatalogDataType() {
        org.apache.doris.catalog.VariantType type = new org.apache.doris.catalog.VariantType(predefinedFields.stream()
                .map(VariantField::toCatalogDataType)
                .collect(Collectors.toCollection(ArrayList::new)), variantMaxSubcolumnsCount, enableTypedPathsToSparse);
        return type;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof VariantType;
    }

    @Override
    public String toSql() {
        if (predefinedFields.isEmpty() && variantMaxSubcolumnsCount == 0) {
            return "variant";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("variant");
        sb.append("<");
        if (!predefinedFields.isEmpty()) {
            sb.append(predefinedFields.stream().map(VariantField::toSql).collect(Collectors.joining(",")));
            if (variantMaxSubcolumnsCount == 0 && !enableTypedPathsToSparse) {
                sb.append(">");
                return sb.toString();
            } else {
                sb.append(",");
            }
        }

        sb.append("PROPERTIES (");
        if (variantMaxSubcolumnsCount != 0) {
            sb.append("\"variant_max_subcolumns_count\" = \"")
                                    .append(String.valueOf(variantMaxSubcolumnsCount)).append("\",");
        }
        if (variantMaxSubcolumnsCount != 0 && enableTypedPathsToSparse) {
            sb.append(",");
        }
        if (enableTypedPathsToSparse) {
            sb.append("\"variant_enable_typed_paths_to_sparse\" = \"")
                                    .append(String.valueOf(enableTypedPathsToSparse)).append("\"");
        }
        sb.append(")>");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantType other = (VariantType) o;
        return this.variantMaxSubcolumnsCount == other.variantMaxSubcolumnsCount
                    && this.enableTypedPathsToSparse == other.enableTypedPathsToSparse
                    && Objects.equals(predefinedFields, other.predefinedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), variantMaxSubcolumnsCount, enableTypedPathsToSparse, predefinedFields);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public List<VariantField> getPredefinedFields() {
        return predefinedFields;
    }

    public int getVariantMaxSubcolumnsCount() {
        return variantMaxSubcolumnsCount;
    }
}
