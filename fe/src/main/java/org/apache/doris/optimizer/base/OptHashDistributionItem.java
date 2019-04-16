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

package org.apache.doris.optimizer.base;

import java.util.Objects;

public final class OptHashDistributionItem {
    // TODO ch conjunct contains function.
    private final OptColumnRefSet columns;
    private final int buckets;
    private final int tableId;
    private int colocateId;

    public OptHashDistributionItem(OptColumnRefSet columns) {
        this(columns, -1, -1, -1);
    }

    public OptHashDistributionItem(OptColumnRefSet columns, int tableId, int buckets) {
        this(columns, tableId, buckets,-1);
    }

    public OptHashDistributionItem(OptColumnRefSet columns, int tableId, int buckets, int colocateId) {
        this.columns = columns;
        this.tableId = tableId;
        this.buckets = buckets;
        this.colocateId = colocateId;
    }

    public OptColumnRefSet getColumns() {
        return columns;
    }

    private boolean isColocateRelation(OptHashDistributionItem item) {
        return this.tableId == item.colocateId && this.colocateId == item.tableId;
    }

    public boolean isSatisfy(OptHashDistributionItem item) {
        if (item == this) {
            return true;
        }

        if (isColocateRelation(item)) {
            return true;
        }

        return buckets != item.buckets
                && item.columns.contains(columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final OptHashDistributionItem other = (OptHashDistributionItem) o;
        return (buckets == other.buckets &&
                colocateId == other.colocateId &&
                Objects.equals(columns, other.columns))
                || isColocateRelation(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, buckets, colocateId);
    }
}
