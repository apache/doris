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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;

public class ForeignKeyConstraint extends Constraint {
    private final ImmutableMap<Column, Column> foreignToReference;
    private final TableIf referencedTable;

    public ForeignKeyConstraint(List<Column> columns,
            TableIf referencedTable, List<Column> referencedColumns) {
        this.referencedTable = referencedTable;
        ImmutableMap.Builder<Column, Column> builder = new Builder<>();
        Preconditions.checkArgument(columns.size() == referencedColumns.size(),
                "Foreign keys' size must be same as the size of reference keys");
        Preconditions.checkArgument(ImmutableSet.copyOf(columns).size() == columns.size(),
                "Foreign keys contains duplicate slots.");
        Preconditions.checkArgument(ImmutableSet.copyOf(referencedColumns).size() == referencedColumns.size(),
                "Reference keys contains duplicate slots.");
        for (int i = 0; i < columns.size(); i++) {
            builder.put(columns.get(i), referencedColumns.get(i));
        }
        this.foreignToReference = builder.build();
    }

    public ImmutableSet<Column> getColumns() {
        return foreignToReference.keySet();
    }

    public ImmutableSet<Column> getReferencedColumns() {
        return ImmutableSet.copyOf(foreignToReference.values());
    }

    public Column getReferencedColumn(Column column) {
        return foreignToReference.get(column);
    }

    public TableIf getReferencedTable() {
        return referencedTable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(foreignToReference, referencedTable);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ForeignKeyConstraint other = (ForeignKeyConstraint) obj;
        return Objects.equals(foreignToReference, other.foreignToReference)
                && Objects.equals(referencedTable, other.referencedTable);
    }
}
