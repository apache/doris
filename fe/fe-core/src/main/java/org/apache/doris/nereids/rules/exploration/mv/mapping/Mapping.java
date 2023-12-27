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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Mapping slot from query to view or inversely,
 * it can also represent the mapping from slot to it's index
 */
public abstract class Mapping {

    /**
     * The relation for mapping
     */
    public static final class MappedRelation {

        public final RelationId relationId;
        public final CatalogRelation belongedRelation;

        public MappedRelation(RelationId relationId, CatalogRelation belongedRelation) {
            this.relationId = relationId;
            this.belongedRelation = belongedRelation;
        }

        public static MappedRelation of(RelationId relationId, CatalogRelation belongedRelation) {
            return new MappedRelation(relationId, belongedRelation);
        }

        public RelationId getRelationId() {
            return relationId;
        }

        public CatalogRelation getBelongedRelation() {
            return belongedRelation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MappedRelation that = (MappedRelation) o;
            return Objects.equals(relationId, that.relationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relationId);
        }
    }

    /**
     * The slot for mapping
     */
    public static final class MappedSlot {

        public final ExprId exprId;
        public final Slot slot;
        @Nullable
        public final CatalogRelation belongedRelation;

        public MappedSlot(ExprId exprId,
                Slot slot,
                CatalogRelation belongedRelation) {
            this.exprId = exprId;
            this.slot = slot;
            this.belongedRelation = belongedRelation;
        }

        public static MappedSlot of(ExprId exprId,
                Slot slot,
                CatalogRelation belongedRelation) {
            return new MappedSlot(exprId, slot, belongedRelation);
        }

        public static MappedSlot of(Slot slot,
                CatalogRelation belongedRelation) {
            return new MappedSlot(slot.getExprId(), slot, belongedRelation);
        }

        public static MappedSlot of(Slot slot) {
            return new MappedSlot(slot.getExprId(), slot, null);
        }

        public ExprId getExprId() {
            return exprId;
        }

        public CatalogRelation getBelongedRelation() {
            return belongedRelation;
        }

        public Slot getSlot() {
            return slot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MappedSlot that = (MappedSlot) o;
            return Objects.equals(exprId, that.exprId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exprId);
        }
    }

    /** Chain fold tow mapping, such as this mapping is {[a -> b]}, the target mapping is
     *  {[b -> c]} after chain fold, this result will be {[a -> c]}, if the value side in this mapping
     *  can get the key in the target mapping, will lose the mapping
     */
    protected Mapping chainedFold(Mapping target) {
        return null;
    }
}
