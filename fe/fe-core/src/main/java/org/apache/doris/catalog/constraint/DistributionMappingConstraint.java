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
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Declares that determinant columns use the named cross-table mapping to determine distribution columns.
 */
public class DistributionMappingConstraint extends Constraint {
    @SerializedName(value = "mi")
    private final String mappingId;
    @SerializedName(value = "dc")
    private final List<String> determinantColumns;
    @SerializedName(value = "tc")
    private final List<String> distributionColumns;
    @SerializedName(value = "sv")
    private final Integer baseSchemaVersion;
    @SerializedName(value = "di")
    private final List<Integer> determinantColumnUniqueIds;
    @SerializedName(value = "ti")
    private final List<Integer> distributionColumnUniqueIds;
    @SerializedName(value = "ds")
    private final List<String> determinantColumnTypeSignatures;
    @SerializedName(value = "ts")
    private final List<String> distributionColumnTypeSignatures;

    /** Constructor. */
    public DistributionMappingConstraint(String name, String mappingId,
            List<String> determinantColumns, List<String> distributionColumns) {
        this(name, mappingId, determinantColumns, distributionColumns,
                null, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    private DistributionMappingConstraint(String name, String mappingId,
            List<String> determinantColumns, List<String> distributionColumns,
            Integer baseSchemaVersion, List<Integer> determinantColumnUniqueIds,
            List<Integer> distributionColumnUniqueIds, List<String> determinantColumnTypeSignatures,
            List<String> distributionColumnTypeSignatures) {
        super(ConstraintType.DISTRIBUTION_MAPPING, name);
        this.mappingId = mappingId;
        this.determinantColumns = ImmutableList.copyOf(determinantColumns);
        this.distributionColumns = ImmutableList.copyOf(distributionColumns);
        this.baseSchemaVersion = baseSchemaVersion;
        this.determinantColumnUniqueIds = ImmutableList.copyOf(determinantColumnUniqueIds);
        this.distributionColumnUniqueIds = ImmutableList.copyOf(distributionColumnUniqueIds);
        this.determinantColumnTypeSignatures = ImmutableList.copyOf(determinantColumnTypeSignatures);
        this.distributionColumnTypeSignatures = ImmutableList.copyOf(distributionColumnTypeSignatures);
    }

    public String getMappingId() {
        return mappingId;
    }

    public List<String> getDeterminantColumnNames() {
        return determinantColumns;
    }

    public List<String> getDistributionColumnNames() {
        return distributionColumns;
    }

    public Integer getBaseSchemaVersion() {
        return baseSchemaVersion;
    }

    public List<Integer> getDeterminantColumnUniqueIds() {
        return determinantColumnUniqueIds;
    }

    public List<Integer> getDistributionColumnUniqueIds() {
        return distributionColumnUniqueIds;
    }

    public List<String> getDeterminantColumnTypeSignatures() {
        return determinantColumnTypeSignatures;
    }

    public List<String> getDistributionColumnTypeSignatures() {
        return distributionColumnTypeSignatures;
    }

    DistributionMappingConstraint bindTo(OlapTable table) {
        return new DistributionMappingConstraint(
                getName(), mappingId, determinantColumns, distributionColumns,
                table.getBaseSchemaVersion(), getColumnUniqueIds(table, determinantColumns),
                getColumnUniqueIds(table, distributionColumns),
                getColumnTypeSignatures(table, determinantColumns),
                getColumnTypeSignatures(table, distributionColumns));
    }

    boolean isCompatibleWith(OlapTable table) {
        if (!hasCompatibleDistributionColumns(table)
                || baseSchemaVersion == null
                || determinantColumnUniqueIds == null
                || distributionColumnUniqueIds == null
                || determinantColumnTypeSignatures == null
                || distributionColumnTypeSignatures == null
                || determinantColumns.size() != determinantColumnUniqueIds.size()
                || distributionColumns.size() != distributionColumnUniqueIds.size()
                || determinantColumns.size() != determinantColumnTypeSignatures.size()
                || distributionColumns.size() != distributionColumnTypeSignatures.size()) {
            return false;
        }
        boolean sameSchemaVersion = baseSchemaVersion == table.getBaseSchemaVersion();
        return columnsMatch(table, determinantColumns, determinantColumnUniqueIds,
                determinantColumnTypeSignatures, sameSchemaVersion)
                && columnsMatch(table, distributionColumns, distributionColumnUniqueIds,
                        distributionColumnTypeSignatures, sameSchemaVersion);
    }

    boolean hasCompatibleDistributionColumns(OlapTable table) {
        if (!(table.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
            return false;
        }
        List<Column> tableDistributionColumns =
                ((HashDistributionInfo) table.getDefaultDistributionInfo()).getDistributionColumns();
        int previousIndex = -1;
        for (String distributionColumn : distributionColumns) {
            int index = -1;
            for (int i = 0; i < tableDistributionColumns.size(); i++) {
                if (tableDistributionColumns.get(i).getName().equalsIgnoreCase(distributionColumn)) {
                    index = i;
                    break;
                }
            }
            if (index <= previousIndex) {
                return false;
            }
            previousIndex = index;
        }
        return true;
    }

    private static List<Integer> getColumnUniqueIds(OlapTable table, List<String> columnNames) {
        ImmutableList.Builder<Integer> uniqueIds = ImmutableList.builder();
        for (String columnName : columnNames) {
            Column column = table.getColumn(columnName);
            Preconditions.checkNotNull(column, "column %s does not exist", columnName);
            uniqueIds.add(column.getUniqueId());
        }
        return uniqueIds.build();
    }

    private static List<String> getColumnTypeSignatures(OlapTable table, List<String> columnNames) {
        ImmutableList.Builder<String> typeSignatures = ImmutableList.builder();
        for (String columnName : columnNames) {
            Column column = table.getColumn(columnName);
            Preconditions.checkNotNull(column, "column %s does not exist", columnName);
            typeSignatures.add(column.getType().toSql());
        }
        return typeSignatures.build();
    }

    private static boolean columnsMatch(OlapTable table, List<String> columnNames,
            List<Integer> expectedUniqueIds, List<String> expectedTypeSignatures, boolean sameSchemaVersion) {
        for (int i = 0; i < columnNames.size(); i++) {
            Column column = table.getColumn(columnNames.get(i));
            if (column == null || !column.getType().toSql().equals(expectedTypeSignatures.get(i))) {
                return false;
            }
            int expectedUniqueId = expectedUniqueIds.get(i);
            if (expectedUniqueId == Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
                if (!sameSchemaVersion) {
                    return false;
                }
            } else if (column.getUniqueId() != expectedUniqueId) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionMappingConstraint that = (DistributionMappingConstraint) o;
        return mappingId.equals(that.mappingId)
                && determinantColumns.equals(that.determinantColumns)
                && distributionColumns.equals(that.distributionColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mappingId, determinantColumns, distributionColumns);
    }

    @Override
    public String toString() {
        return String.format("COLOCATE MAPPING %s (%s) DETERMINES DISTRIBUTION KEY (%s) NOT ENFORCED",
                mappingId, String.join(", ", determinantColumns), String.join(", ", distributionColumns));
    }
}
