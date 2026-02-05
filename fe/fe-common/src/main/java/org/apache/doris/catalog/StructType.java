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

import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Describes a STRUCT type. STRUCT types have a list of named struct fields.
 */
public class StructType extends Type {

    @SerializedName(value = "fieldMap")
    private final HashMap<String, StructField> fieldMap = Maps.newHashMap();

    @SerializedName(value = "fields")
    private final ArrayList<StructField> fields;

    public StructType(ArrayList<StructField> fields) {
        Preconditions.checkNotNull(fields);
        this.fields = fields;
        for (int i = 0; i < this.fields.size(); ++i) {
            this.fields.get(i).setPosition(i);
            fieldMap.put(this.fields.get(i).getName().toLowerCase(), this.fields.get(i));
        }
    }

    public StructType(StructField... fields) {
        this(new ArrayList<>(Arrays.asList(fields)));
    }

    public StructType(List<Type> types) {
        Preconditions.checkNotNull(types);
        ArrayList<StructField> newFields = new ArrayList<>();
        for (Type type : types) {
            newFields.add(new StructField(type));
        }
        this.fields = newFields;
    }

    public StructType(Type... types) {
        this(Arrays.asList(types));
    }

    public StructType() {
        this.fields = Lists.newArrayList();
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "struct<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toSql(depth + 1));
        }
        return String.format("struct<%s>", Joiner.on(",").join(fieldsSql));
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.prettyPrint(lpad + 2));
        }
        return String.format("%sSTRUCT<\n%s\n%s>",
                leftPadding, Joiner.on(",\n").join(fieldsSql), leftPadding);
    }

    @Override
    public boolean isSupported() {
        for (StructField f : fields) {
            if (!f.getType().isSupported()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean supportSubType(Type subType) {
        for (Type supportedType : Type.getStructSubTypes()) {
            if (subType.getPrimitiveType() == supportedType.getPrimitiveType()) {
                return true;
            }
        }
        return false;
    }

    public void addField(StructField field) {
        field.setPosition(fields.size());
        fields.add(field);
        fieldMap.put(field.getName().toLowerCase(), field);
    }

    public ArrayList<StructField> getFields() {
        return fields;
    }

    public StructField getField(String fieldName) {
        return fieldMap.get(fieldName.toLowerCase());
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.STRUCT;
    }

    @Override
    public boolean matchesType(Type t) {
        if (equals(t)) {
            return true;
        }

        if (t.isAnyType()) {
            return t.matchesType(this);
        }

        if (!t.isStructType()) {
            return false;
        }

        StructType other = (StructType) t;
        // Temp to make NullPredict from fe send to be
        if (other.getFields().size() == 1 && Objects.equals(other.getFields().get(0).name,
                Type.GENERIC_STRUCT.getFields().get(0).name)) {
            return true;
        }
        if (fields.size() != other.getFields().size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).matchesField(((StructType) t).getFields().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StructType)) {
            return false;
        }
        StructType otherStructType = (StructType) other;
        return otherStructType.getFields().equals(fields);
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(fields);
        Preconditions.checkState(!fields.isEmpty());
        node.setType(TTypeNodeType.STRUCT);
        node.setStructFields(new ArrayList<>());
        for (StructField field : fields) {
            field.toThrift(container, node);
        }
    }

    @Override
    public String toString() {
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toString());
        }
        return String.format("struct<%s>", Joiner.on(",").join(fieldsSql));
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = PrimitiveType.STRUCT.toThrift();
        return thrift;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.STRUCT.getSlotSize();
    }
}
