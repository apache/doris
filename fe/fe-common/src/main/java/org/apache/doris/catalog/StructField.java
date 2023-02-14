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

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

/**
 * TODO: Support comments for struct fields. The Metastore does not properly store
 * comments of struct fields. We set comment to null to avoid compatibility issues.
 */
public class StructField {
    @SerializedName(value = "name")
    protected final String name;

    @SerializedName(value = "type")
    protected final Type type;

    @SerializedName(value = "comment")
    protected final String comment;

    @SerializedName(value = "position")
    protected int position;  // in struct

    @SerializedName(value = "containsNull")
    private final boolean containsNull; // Now always true (nullable field)

    private static final String DEFAULT_FIELD_NAME = "col";

    public StructField(String name, Type type, String comment, boolean containsNull) {
        this.name = name.toLowerCase();
        this.type = type;
        this.comment = comment;
        this.containsNull = containsNull;
    }

    public StructField(String name, Type type) {
        this(name, type, null, true);
    }

    public StructField(Type type) {
        this(DEFAULT_FIELD_NAME, type, null, true);
    }

    public String getComment() {
        return comment;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public boolean getContainsNull() {
        return containsNull;
    }

    public String toSql(int depth) {
        String typeSql;
        if (depth < Type.MAX_NESTING_DEPTH) {
            typeSql = !containsNull ? "not_null(" + type.toSql(depth) + ")" : type.toSql(depth);
        } else {
            typeSql = "...";
        }
        StringBuilder sb = new StringBuilder(name);
        if (type != null) {
            sb.append(":").append(typeSql);
        }
        if (comment != null) {
            sb.append(String.format(" COMMENT '%s'", comment));
        }
        return sb.toString();
    }

    /**
     * Pretty prints this field with lpad number of leading spaces.
     * Calls prettyPrint(lpad) on this field's type.
     */
    public String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        StringBuilder sb = new StringBuilder(leftPadding + name);
        if (type != null) {
            // Pass in the padding to make sure nested fields are aligned properly,
            // even if we then strip the top-level padding.
            String typeStr = type.prettyPrint(lpad);
            typeStr = typeStr.substring(lpad);
            sb.append(":").append(typeStr);
        }
        if (comment != null) {
            sb.append(String.format(" COMMENT '%s'", comment));
        }
        return sb.toString();
    }

    public static boolean canCastTo(StructField field, StructField targetField) {
        // TODO(xy): support cast field
        return false;
    }

    public boolean matchesField(StructField f) {
        if (equals(f)) {
            return true;
        }
        return type.matchesType(f.getType()) && containsNull == f.getContainsNull();
    }

    public void toThrift(TTypeDesc container, TTypeNode node) {
        TStructField field = new TStructField();
        field.setName(name);
        if (comment != null) {
            field.setComment(comment);
        }
        field.setContainsNull(containsNull);
        node.struct_fields.add(field);
        type.toThrift(container);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StructField)) {
            return false;
        }
        StructField otherStructField = (StructField) other;
        return otherStructField.name.equals(name) && otherStructField.type.equals(type)
                && otherStructField.containsNull == containsNull;
    }
}
