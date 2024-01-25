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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PrimaryKeyConstraint extends Constraint {
    @SerializedName(value = "cols")
    private final Set<String> columns;

    // record the foreign table which references the primary key
    @SerializedName(value = "ft")
    private final Set<TableIdentifier> foreignTables = new HashSet<>();

    public PrimaryKeyConstraint(String name, Set<String> columns) {
        super(ConstraintType.PRIMARY_KEY, name);
        this.columns = ImmutableSet.copyOf(columns);
    }

    public Set<String> getPrimaryKeyNames() {
        return columns;
    }

    public Set<Column> getPrimaryKeys(TableIf table) {
        return columns.stream().map(table::getColumn).collect(ImmutableSet.toImmutableSet());
    }

    public void addForeignTable(TableIf table) {
        foreignTables.add(new TableIdentifier(table));
    }

    public List<TableIf> getForeignTables() {
        return foreignTables.stream()
                .map(TableIdentifier::toTableIf)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return "PRIMARY KEY (" + String.join(", ", columns) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryKeyConstraint that = (PrimaryKeyConstraint) o;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns);
    }
}
