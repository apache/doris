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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class ForeignKeyConstraint extends Constraint implements GsonPostProcessable {
    @SerializedName(value = "ftr")
    private final Map<String, String> foreignToReference;
    @SerializedName(value = "rt")
    private final TableIdentifier referencedTable;

    // qualified name string kept for backward-compatible deserialization
    @SerializedName(value = "rtn")
    private String referencedTableNameStr;

    @SerializedName(value = "rtni")
    private volatile TableNameInfo referencedTableInfo;

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
        this.referencedTableInfo = new TableNameInfo(
                refTable.getDatabase().getCatalog().getName(),
                refTable.getDatabase().getFullName(),
                refTable.getName());
        this.referencedTableNameStr = referencedTableInfo.getCtl() + "."
                + referencedTableInfo.getDb() + "." + referencedTableInfo.getTbl();
        for (int i = 0; i < columns.size(); i++) {
            builder.put(columns.get(i), referencedColumns.get(i));
        }
        this.foreignToReference = builder.build();
    }

    public ForeignKeyConstraint(String name, List<String> columns,
            TableNameInfo referencedTableInfo, List<String> referencedColumns) {
        super(ConstraintType.FOREIGN_KEY, name);
        ImmutableMap.Builder<String, String> builder = new Builder<>();
        Preconditions.checkArgument(columns.size() == referencedColumns.size(),
                "Foreign keys' size must be same as the size of reference keys");
        Preconditions.checkArgument(ImmutableSet.copyOf(columns).size() == columns.size(),
                "Foreign keys contains duplicate slots.");
        Preconditions.checkArgument(ImmutableSet.copyOf(referencedColumns).size() == referencedColumns.size(),
                "Reference keys contains duplicate slots.");
        this.referencedTable = null;
        this.referencedTableInfo = referencedTableInfo;
        this.referencedTableNameStr = referencedTableInfo.getCtl() + "."
                + referencedTableInfo.getDb() + "." + referencedTableInfo.getTbl();
        for (int i = 0; i < columns.size(); i++) {
            builder.put(columns.get(i), referencedColumns.get(i));
        }
        this.foreignToReference = builder.build();
    }

    public Set<String> getForeignKeyNames() {
        return foreignToReference.keySet();
    }

    public Set<String> getPrimaryKeyNames() {
        return ImmutableSet.copyOf(foreignToReference.values());
    }

    public Set<String> getReferencedColumnNames() {
        return ImmutableSet.copyOf(foreignToReference.values());
    }

    public String getReferencedColumnName(String column) {
        return foreignToReference.get(column);
    }

    public Map<String, String> getForeignToReference() {
        return foreignToReference;
    }

    public Map<Column, Column> getForeignToPrimary(TableIf curTable) {
        ImmutableMap.Builder<Column, Column> columnBuilder = new ImmutableMap.Builder<>();
        TableIf refTable = resolveReferencedTable();
        foreignToReference.forEach((k, v) ->
                columnBuilder.put(curTable.getColumn(k), refTable.getColumn(v)));
        return columnBuilder.build();
    }

    public Column getReferencedColumn(String column) {
        return resolveReferencedTable().getColumn(getReferencedColumnName(column));
    }

    public TableIf getReferencedTable() {
        return resolveReferencedTable();
    }

    public Optional<TableIf> getReferencedTableOrNull() {
        try {
            return Optional.of(resolveReferencedTable());
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private TableIf resolveReferencedTable() {
        if (referencedTable != null) {
            try {
                return referencedTable.toTableIf();
            } catch (Exception e) {
                // fall through to name-based resolution
            }
        }
        if (referencedTableInfo != null) {
            try {
                return Env.getCurrentEnv().getCatalogMgr()
                        .getCatalog(referencedTableInfo.getCtl())
                        .getDbOrAnalysisException(referencedTableInfo.getDb())
                        .getTableOrAnalysisException(referencedTableInfo.getTbl());
            } catch (Exception e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "Cannot resolve referenced table: " + referencedTableInfo, e);
            }
        }
        Preconditions.checkNotNull(referencedTableNameStr,
                "Neither referencedTable nor referencedTableInfo/referencedTableNameStr is set");
        String[] parts = referencedTableNameStr.split("\\.", 3);
        Preconditions.checkArgument(parts.length == 3,
                "Invalid qualified table name: %s", referencedTableNameStr);
        try {
            return Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(parts[0]).getDbOrAnalysisException(parts[1])
                    .getTableOrAnalysisException(parts[2]);
        } catch (Exception e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Cannot resolve referenced table: " + referencedTableNameStr, e);
        }
    }

    public TableNameInfo getReferencedTableName() {
        return referencedTableInfo;
    }

    public void setReferencedTableInfo(TableNameInfo info) {
        this.referencedTableInfo = info;
        this.referencedTableNameStr = info.getCtl() + "." + info.getDb() + "." + info.getTbl();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (referencedTableInfo == null && referencedTableNameStr != null) {
            referencedTableInfo = new TableNameInfo(referencedTableNameStr);
        }
        if (referencedTableInfo == null && referencedTable != null) {
            try {
                String qualifiedName = referencedTable.toQualifiedName();
                if (qualifiedName != null) {
                    referencedTableNameStr = qualifiedName;
                    referencedTableInfo = new TableNameInfo(qualifiedName);
                }
            } catch (Exception ignored) {
                // skip if the referenced table can no longer be resolved
            }
        }
    }

    public Boolean isReferringPK(TableNameInfo pkTableInfo, PrimaryKeyConstraint constraint) {
        return constraint.getPrimaryKeyNames().equals(getPrimaryKeyNames())
                && pkTableInfo.equals(referencedTableInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(foreignToReference, referencedTableInfo);
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
                && Objects.equals(referencedTableInfo, other.referencedTableInfo);
    }

    @Override
    public String toString() {
        String foreignKeys = "(" + String.join(", ", foreignToReference.keySet()) + ")";
        String primaryKeys = "(" + String.join(", ", foreignToReference.values()) + ")";
        return String.format("FOREIGN KEY %s REFERENCES %s %s",
                foreignKeys, referencedTableInfo, primaryKeys);
    }

    public String getTypeName() {
        return "FOREIGN KEY";
    }
}
