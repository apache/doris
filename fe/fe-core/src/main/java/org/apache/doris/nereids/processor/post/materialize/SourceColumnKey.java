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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Stable identity of one physical source column used by TopN lazy materialization. */
public final class SourceColumnKey implements Comparable<SourceColumnKey> {
    private final RelationId relationId;
    private final int columnUniqueId;
    private final String columnName;
    private final List<ColumnAccessPath> accessPaths;

    private SourceColumnKey(RelationId relationId, int columnUniqueId, String columnName,
            List<ColumnAccessPath> accessPaths) {
        this.relationId = Objects.requireNonNull(relationId, "relationId must not be null");
        this.columnUniqueId = columnUniqueId;
        this.columnName = Objects.requireNonNull(columnName, "columnName must not be null");
        this.accessPaths = ImmutableList.copyOf(accessPaths);
    }

    /** Build a key from resolved relation and base slot metadata. */
    public static SourceColumnKey from(Relation relation, SlotReference baseSlot) {
        Objects.requireNonNull(relation, "relation must not be null");
        Objects.requireNonNull(baseSlot, "baseSlot must not be null");
        Preconditions.checkArgument(baseSlot.getOriginalColumn().isPresent(),
                "baseSlot must have an original column: %s", baseSlot);
        Column column = baseSlot.getOriginalColumn().get();
        List<ColumnAccessPath> normalizedPaths = new ArrayList<>(
                baseSlot.getAllAccessPaths().orElse(ImmutableList.of()));
        Collections.sort(normalizedPaths);
        return new SourceColumnKey(relation.getRelationId(), column.getUniqueId(), column.getName(), normalizedPaths);
    }

    public RelationId getRelationId() {
        return relationId;
    }

    public int getColumnUniqueId() {
        return columnUniqueId;
    }

    public String getColumnName() {
        return columnName;
    }

    public List<ColumnAccessPath> getAccessPaths() {
        return accessPaths;
    }

    @Override
    public int compareTo(SourceColumnKey other) {
        int result = Integer.compare(relationId.asInt(), other.relationId.asInt());
        if (result != 0) {
            return result;
        }
        result = Integer.compare(columnUniqueId, other.columnUniqueId);
        if (result != 0) {
            return result;
        }
        result = columnName.compareTo(other.columnName);
        if (result != 0) {
            return result;
        }
        int commonSize = Math.min(accessPaths.size(), other.accessPaths.size());
        for (int i = 0; i < commonSize; i++) {
            result = accessPaths.get(i).compareTo(other.accessPaths.get(i));
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(accessPaths.size(), other.accessPaths.size());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SourceColumnKey)) {
            return false;
        }
        SourceColumnKey other = (SourceColumnKey) object;
        return columnUniqueId == other.columnUniqueId
                && relationId.equals(other.relationId)
                && columnName.equals(other.columnName)
                && accessPaths.equals(other.accessPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationId, columnUniqueId, columnName, accessPaths);
    }

    @Override
    public String toString() {
        return relationId + ":" + columnUniqueId + ":" + columnName + accessPaths;
    }
}
