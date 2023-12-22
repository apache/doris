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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ForeignKeyConstraint extends Constraint {
    private final ImmutableMap<String, String> foreignToReference;
    private final TableIdentifier referencedTable;

    public ForeignKeyConstraint(String name, List<String> columns,
            TableIf refTable, List<String> referencedColumns) {
        super(ConstraintType.FOREIGN_KEY, name);
        ImmutableMap.Builder<String, String> builder = new Builder<>();
        Preconditions.checkArgument(columns.size() == referencedColumns.size(),
                "Foreign keys' size must be same as the size of reference keys");
        Preconditions.checkArgument(ImmutableSet.copyOf(columns).size() == columns.size(),
                "Foreign keys contains duplicate slots.");
        Preconditions.checkArgument(ImmutableSet.copyOf(referencedColumns).size() == referencedColumns.size(),
                "Reference keys contains duplicate slots.");
        this.referencedTable = new TableIdentifier(refTable);
        for (int i = 0; i < columns.size(); i++) {
            builder.put(columns.get(i), referencedColumns.get(i));
        }
        this.foreignToReference = builder.build();
    }

    public ImmutableSet<String> getForeignKeyNames() {
        return foreignToReference.keySet();
    }

    public ImmutableSet<String> getReferencedColumnNames() {
        return ImmutableSet.copyOf(foreignToReference.values());
    }

    public String getReferencedColumnName(String column) {
        return foreignToReference.get(column);
    }

    public ImmutableMap<String, String> getForeignToReference() {
        return foreignToReference;
    }

    public Map<Column, Column> getForeignToPrimary(TableIf curTable) {
        ImmutableMap.Builder<Column, Column> columnBuilder = new ImmutableMap.Builder<>();
        TableIf refTable = referencedTable.toTableIf();
        foreignToReference.forEach((k, v) ->
                columnBuilder.put(curTable.getColumn(k), refTable.getColumn(v)));
        return columnBuilder.build();
    }

    public Column getReferencedColumn(String column) {
        return getReferencedTable().getColumn(getReferencedColumnName(column));
    }

    public TableIf getReferencedTable() {
        return referencedTable.toTableIf();
    }

    public Boolean isReferringPK(TableIf table, PrimaryKeyConstraint constraint) {
        return constraint.getPrimaryKeyNames().equals(getForeignKeyNames())
                && getReferencedTable().equals(table);
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
