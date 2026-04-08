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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** PartialAggType */
public class PartialAggType extends DataType {
    public final List<DataType> intermediateTypes;

    /** PartialAggType */
    public PartialAggType(List<DataType> intermediateTypes) {
        this.intermediateTypes = ImmutableList.copyOf(
                Objects.requireNonNull(intermediateTypes, "intermediateTypes can not be null"));
        Preconditions.checkArgument(!intermediateTypes.isEmpty(), "intermediateTypes can not empty");
    }

    public List<DataType> getIntermediateTypes() {
        return intermediateTypes;
    }

    @Override
    public String toSql() {
        return "PartialAggType(types=" + intermediateTypes + ")";
    }

    @Override
    public int width() {
        return intermediateTypes.stream()
                .map(DataType::width)
                .reduce(Integer::sum)
                .get();
    }

    @Override
    public Type toCatalogDataType() {
        if (intermediateTypes.size() == 1) {
            return intermediateTypes.get(0).toCatalogDataType();
        }
        return ScalarType.createVarcharType(-1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PartialAggType that = (PartialAggType) o;
        return Objects.equals(intermediateTypes, that.intermediateTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), intermediateTypes);
    }
}
