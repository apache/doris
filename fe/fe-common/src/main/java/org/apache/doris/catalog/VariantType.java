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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTypeDesc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class VariantType extends ScalarType {
    private static final Logger LOG = LogManager.getLogger(VariantType.class);
    @SerializedName(value = "fieldMap")
    private final HashMap<String, VariantField> fieldMap = Maps.newHashMap();

    @SerializedName(value = "fields")
    private final ArrayList<VariantField> predefinedFields;

    @SerializedName(value = "variantMaxSubcolumnsCount")
    private int variantMaxSubcolumnsCount;

    public VariantType() {
        super(PrimitiveType.VARIANT);
        this.predefinedFields = Lists.newArrayList();
    }

    public VariantType(ArrayList<VariantField> fields) {
        super(PrimitiveType.VARIANT);
        Preconditions.checkNotNull(fields);
        this.predefinedFields = fields;
        for (VariantField predefinedField : this.predefinedFields) {
            fieldMap.put(predefinedField.getPattern(), predefinedField);
        }
    }

    @Override
    public String toSql(int depth) {
        if (predefinedFields.isEmpty()) {
            return "variant";
        }
        if (depth >= MAX_NESTING_DEPTH) {
            return "variant<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (VariantField f : predefinedFields) {
            fieldsSql.add(f.toSql(depth + 1));
        }
        return String.format("variant<%s>", Joiner.on(",").join(fieldsSql));
    }

    public ArrayList<VariantField> getPredefinedFields() {
        return predefinedFields;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        super.toThrift(container);
        // set the count
        container.getTypes().get(container.getTypes().size() - 1)
                .scalar_type.setVariantMaxSubcolumnsCount(variantMaxSubcolumnsCount);
    }

    @Override
    public boolean supportSubType(Type subType) {
        for (Type supportedType : Type.getVariantSubTypes()) {
            if (subType.getPrimitiveType() == supportedType.getPrimitiveType()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VariantType)) {
            return false;
        }
        VariantType otherVariantType = (VariantType) other;
        return Objects.equals(otherVariantType.getPredefinedFields(), predefinedFields)
                && variantMaxSubcolumnsCount == otherVariantType.variantMaxSubcolumnsCount;
    }

    @Override
    public boolean matchesType(Type type) {
        return type.isVariantType();
    }

    public void setVariantMaxSubcolumnsCount(int variantMaxSubcolumnsCount) {
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
    }

    public int getVariantMaxSubcolumnsCount() {
        return variantMaxSubcolumnsCount;
    }
}
