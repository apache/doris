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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Definition of a dictionary column.
 */
public class DictionaryColumnDefinition {
    @SerializedName(value = "name")
    private final String name;
    @SerializedName(value = "isKey")
    private final boolean isKey;
    @SerializedName(value = "type")
    private Type type;
    @SerializedName(value = "nullable")
    private boolean nullable;

    // map to origin column in source table.
    // TODO: consider of source table's schema change
    @SerializedName(value = "originColumn")
    private Column originColumn;

    public DictionaryColumnDefinition(String name, boolean isKey) {
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.isKey = isKey;
    }

    public DictionaryColumnDefinition(String name, boolean isKey, Type type) {
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.isKey = isKey;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public boolean isKey() {
        return isKey;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public void setOriginColumn(Column originColumn) {
        this.originColumn = originColumn;
    }

    public Column getOriginColumn() {
        return originColumn;
    }
}
