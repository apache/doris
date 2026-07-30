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

import com.google.common.base.Objects;
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

    /** Constructor. */
    public DistributionMappingConstraint(String name, String mappingId,
            List<String> determinantColumns, List<String> distributionColumns) {
        super(ConstraintType.DISTRIBUTION_MAPPING, name);
        this.mappingId = mappingId;
        this.determinantColumns = ImmutableList.copyOf(determinantColumns);
        this.distributionColumns = ImmutableList.copyOf(distributionColumns);
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
