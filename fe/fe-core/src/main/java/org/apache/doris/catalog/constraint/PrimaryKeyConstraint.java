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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PrimaryKeyConstraint extends Constraint implements GsonPostProcessable {
    @SerializedName(value = "cols")
    private final Set<String> columns;

    // record the foreign table which references the primary key
    @SerializedName(value = "ft")
    private Set<TableIdentifier> foreignTables = new HashSet<>();

    // qualified name strings kept for backward-compatible deserialization
    @SerializedName(value = "ftn")
    private Set<String> foreignTableNameStrs = new HashSet<>();

    @SerializedName(value = "ftni")
    private List<TableNameInfo> foreignTableInfos = new ArrayList<>();

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

    public void addForeignTable(TableNameInfo tni) {
        String key = tni.getCtl() + "." + tni.getDb() + "." + tni.getTbl();
        if (foreignTableNameStrs.add(key)) {
            foreignTableInfos.add(tni);
        }
    }

    /**
     * @deprecated Use {@link #getForeignTableInfos()} instead.
     *     Returns empty for constraints created via the new ConstraintManager.
     */
    @Deprecated
    public List<TableIf> getForeignTables() {
        return foreignTables.stream()
                .map(TableIdentifier::toTableIf)
                .collect(ImmutableList.toImmutableList());
    }

    public void removeForeignTable(TableIdentifier tableIdentifier) {
        foreignTables.remove(tableIdentifier);
    }

    public void removeForeignTable(TableNameInfo tni) {
        String key = tni.getCtl() + "." + tni.getDb() + "." + tni.getTbl();
        foreignTableNameStrs.remove(key);
        foreignTableInfos.removeIf(info ->
                java.util.Objects.equals(info.getCtl(), tni.getCtl())
                        && java.util.Objects.equals(info.getDb(), tni.getDb())
                        && java.util.Objects.equals(info.getTbl(), tni.getTbl()));
    }

    public List<TableNameInfo> getForeignTableInfos() {
        return Collections.unmodifiableList(foreignTableInfos);
    }

    public void renameForeignTable(TableNameInfo oldInfo, TableNameInfo newInfo) {
        String oldKey = oldInfo.getCtl() + "." + oldInfo.getDb() + "." + oldInfo.getTbl();
        if (foreignTableNameStrs.remove(oldKey)) {
            String newKey = newInfo.getCtl() + "." + newInfo.getDb() + "." + newInfo.getTbl();
            foreignTableNameStrs.add(newKey);
        }
        for (int i = 0; i < foreignTableInfos.size(); i++) {
            TableNameInfo info = foreignTableInfos.get(i);
            if (java.util.Objects.equals(info.getCtl(), oldInfo.getCtl())
                    && java.util.Objects.equals(info.getDb(), oldInfo.getDb())
                    && java.util.Objects.equals(info.getTbl(), oldInfo.getTbl())) {
                foreignTableInfos.set(i, newInfo);
                break;
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (foreignTables == null) {
            foreignTables = new HashSet<>();
        }
        if (foreignTableNameStrs == null) {
            foreignTableNameStrs = new HashSet<>();
        }
        if (foreignTableInfos == null) {
            foreignTableInfos = new ArrayList<>();
        }
        if (foreignTableInfos.isEmpty() && !foreignTableNameStrs.isEmpty()) {
            for (String qualifiedName : foreignTableNameStrs) {
                foreignTableInfos.add(new TableNameInfo(qualifiedName));
            }
        }
        if (foreignTableInfos.isEmpty() && !foreignTables.isEmpty()) {
            for (TableIdentifier tableIdentifier : foreignTables) {
                try {
                    String qualifiedName = tableIdentifier.toQualifiedName();
                    if (qualifiedName != null) {
                        foreignTableNameStrs.add(qualifiedName);
                        foreignTableInfos.add(new TableNameInfo(qualifiedName));
                    }
                } catch (Exception ignored) {
                    // skip entries that can no longer be resolved
                }
            }
        }
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
