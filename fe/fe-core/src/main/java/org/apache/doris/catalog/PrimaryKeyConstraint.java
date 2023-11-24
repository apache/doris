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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKeyConstraint extends Constraint {
    private final ImmutableSet<Column> columns;

    // record the foreign table which references the primary key
    private final List<TableIf> foreignTables = new ArrayList<>();

    public PrimaryKeyConstraint(ImmutableSet<Column> columns) {
        this.columns = columns;
    }

    public ImmutableSet<Column> getColumns() {
        return columns;
    }

    public void addForeignTable(TableIf table) {
        foreignTables.add(table);
    }

    public List<TableIf> getReferenceTables() {
        return ImmutableList.copyOf(foreignTables);
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
