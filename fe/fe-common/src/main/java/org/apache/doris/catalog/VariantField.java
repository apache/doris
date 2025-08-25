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

import org.apache.doris.thrift.TPatternType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

public class VariantField {

    @SerializedName(value = "fp")
    protected final String pattern;

    @SerializedName(value = "ft")
    protected final Type type;

    @SerializedName(value = "fc")
    protected final String comment;

    @SerializedName(value = "fpt")
    protected final TPatternType patternType;

    public VariantField(String pattern, Type type, String comment, TPatternType patternType) {
        this.pattern = pattern;
        this.type = type;
        this.comment = comment;
        this.patternType = patternType;
    }

    // default MATCH_GLOB
    public VariantField(String pattern, Type type, String comment) {
        this(pattern, type, comment, TPatternType.MATCH_NAME_GLOB);
    }

    public Type getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    public String getComment() {
        return comment;
    }

    public TPatternType getPatternType() {
        return patternType;
    }

    public String toSql(int depth) {
        StringBuilder sb = new StringBuilder();
        if (patternType == TPatternType.MATCH_NAME) {
            sb.append(patternType.toString()).append(" ");
        }

        sb.append("'").append(pattern).append("'");
        sb.append(":").append(type.toSql(depth + 1));
        if (!comment.isEmpty()) {
            sb.append(" COMMENT '").append(comment).append("'");
        }
        return sb.toString();
    }

    /**
     * Pretty prints this field with lpad number of leading spaces.
     * Calls prettyPrint(lpad) on this field's type.
     */
    public String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        StringBuilder sb = new StringBuilder(leftPadding + pattern);
        if (type != null) {
            // Pass in the padding to make sure nested fields are aligned properly,
            // even if we then strip the top-level padding.
            String typeStr = type.prettyPrint(lpad);
            typeStr = typeStr.substring(lpad);
            sb.append(":").append(typeStr);
        }
        return sb.toString();
    }

    public static boolean canCastTo(VariantField field, VariantField targetField) {
        if (targetField.type.isStringType() && field.type.isStringType()) {
            return true;
        }
        return Type.canCastTo(field.type, targetField.type);
    }

    public boolean matchesField(VariantField f) {
        if (equals(f)) {
            return true;
        }
        return type.matchesType(f.getType());
    }

    public void toThrift(TTypeDesc container, TTypeNode node) {
        type.toThrift(container);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VariantField)) {
            return false;
        }
        VariantField otherFiled = (VariantField) other;
        return otherFiled.pattern.equals(pattern) && otherFiled.type.equals(type);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(pattern);
        if (type != null) {
            sb.append(":").append(type);
        }
        return sb.toString();
    }
}
