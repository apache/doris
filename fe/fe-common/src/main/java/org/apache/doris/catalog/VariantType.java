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
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;

public class VariantType extends ScalarType {

    @SerializedName(value = "fieldMap")
    private final HashMap<String, StructField> fieldMap = Maps.newHashMap();

    @SerializedName(value = "fields")
    private final ArrayList<StructField> predefinedFields;

    public VariantType() {
        super(PrimitiveType.VARIANT);
        this.predefinedFields = Lists.newArrayList();
    }

    public VariantType(ArrayList<StructField> fields) {
        super(PrimitiveType.VARIANT);
        Preconditions.checkNotNull(fields);
        this.predefinedFields = fields;
        for (int i = 0; i < this.predefinedFields.size(); ++i) {
            this.predefinedFields.get(i).setPosition(i);
            fieldMap.put(this.predefinedFields.get(i).getName(), this.predefinedFields.get(i));
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
        for (StructField f : predefinedFields) {
            fieldsSql.add(f.toSql(depth + 1));
        }
        return String.format("variant<%s>", Joiner.on(",").join(fieldsSql));
    }

    public ArrayList<StructField> getPredefinedFields() {
        return predefinedFields;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        // use ScalarType's toThrift for compatibility, because VariantType use ScalarType to thrift previously
        if (predefinedFields.isEmpty()) {
            super.toThrift(container);
            return;
        }
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        node.setType(TTypeNodeType.VARIANT);
        // predefined fields
        node.setStructFields(new ArrayList<>());
        for (StructField field : predefinedFields) {
            field.toThrift(container, node);
        }
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
        return otherVariantType.getPredefinedFields().equals(predefinedFields);
    }

    @Override
    public boolean matchesType(Type type) {
        return type.isVariantType();
    }
}
