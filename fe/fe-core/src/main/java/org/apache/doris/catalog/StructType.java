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

import org.apache.doris.thrift.TStructField;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Describes a STRUCT type. STRUCT types have a list of named struct fields.
 */
public class StructType extends Type {
    private final HashMap<String, StructField> fieldMap = Maps.newHashMap();
    private final ArrayList<StructField> fields;

    public StructType(ArrayList<StructField> fields) {
        Preconditions.checkNotNull(fields);
        this.fields = fields;
        for (int i = 0; i < this.fields.size(); ++i) {
            this.fields.get(i).setPosition(i);
            fieldMap.put(this.fields.get(i).getName().toLowerCase(), this.fields.get(i));
        }
    }

    public StructType() {
        fields = Lists.newArrayList();
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "STRUCT<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toSql(depth + 1));
        }
        return String.format("STRUCT<%s>", Joiner.on(",").join(fieldsSql));
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

    public void clearFields() {
        fields.clear();
        fieldMap.clear();
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
        Preconditions.checkNotNull(!fields.isEmpty());
        node.setType(TTypeNodeType.STRUCT);
        node.setStructFields(new ArrayList<TStructField>());
        for (StructField field : fields) {
            field.toThrift(container, node);
        }
    }
    @Override
    public String toString() {
        return toSql(0);
    }
}

