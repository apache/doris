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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.Relation;

import java.util.Objects;

/** Resolved physical source for one lazy output. */
public final class MaterializeSource {
    private final Relation relation;
    private final SlotReference baseSlot;
    private final SourceColumnKey sourceColumnKey;

    public MaterializeSource(Relation relation, SlotReference baseSlot) {
        this.relation = Objects.requireNonNull(relation, "relation must not be null");
        this.baseSlot = Objects.requireNonNull(baseSlot, "baseSlot must not be null");
        this.sourceColumnKey = SourceColumnKey.from(relation, baseSlot);
    }

    public Relation getRelation() {
        return relation;
    }

    public SlotReference getBaseSlot() {
        return baseSlot;
    }

    public SourceColumnKey getSourceColumnKey() {
        return sourceColumnKey;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MaterializeSource)) {
            return false;
        }
        MaterializeSource other = (MaterializeSource) object;
        return sourceColumnKey.equals(other.sourceColumnKey) && baseSlot.equals(other.baseSlot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceColumnKey, baseSlot);
    }
}
