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

import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        // Generate eagerly, will be used to generate slot mapping.
        private final Map<RelationSemanticSlotKey, SlotReference> slotBySemanticKey = new HashMap<>();

        /**
         * Construct relation and slot map
         */
        public MappedRelation(RelationId relationId, CatalogRelation belongedRelation) {
            this.relationId = relationId;
            this.belongedRelation = belongedRelation;
            for (Slot slot : belongedRelation.getOutput()) {
                if (slot instanceof SlotReference) {
                    putSlotBySemanticKey((SlotReference) slot);
                }
            }
            for (Expression predicate : belongedRelation.getRelationImpliedPredicates()) {
                List<SlotReference> slots = predicate.collectToList(SlotReference.class::isInstance);
                for (SlotReference slot : slots) {
                    putSlotBySemanticKey(slot);
                }
            }
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

        Map<RelationSemanticSlotKey, SlotReference> getSlotBySemanticKey() {
            return slotBySemanticKey;
        }

        private void putSlotBySemanticKey(SlotReference slot) {
            if (slot.getOriginalTable().isPresent() && slot.getOriginalColumn().isPresent()) {
                slotBySemanticKey.putIfAbsent(RelationSemanticSlotKey.of(slot), slot);
            }
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

        @Override
        public String toString() {
            String relationTableName = belongedRelation.getTable().getNameWithFullQualifiers();
            return "MappedRelation{" + "relationId=" + relationId + ", relationTableName="
                    + relationTableName + ", slotBySemanticKey=" + slotBySemanticKey + '}';
        }
    }

    static final class RelationSemanticSlotKey {
        private final TableIdentifier tableIdentifier;
        private final String columnName;
        private final List<String> subPath;

        private RelationSemanticSlotKey(TableIdentifier tableIdentifier, String columnName, List<String> subPath) {
            this.tableIdentifier = tableIdentifier;
            this.columnName = columnName;
            this.subPath = subPath;
        }

        private static RelationSemanticSlotKey of(SlotReference slot) {
            return new RelationSemanticSlotKey(
                    new TableIdentifier(slot.getOriginalTable().get()),
                    slot.getOriginalColumn().get().getName(),
                    slot.getSubPath());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RelationSemanticSlotKey)) {
                return false;
            }
            RelationSemanticSlotKey that = (RelationSemanticSlotKey) o;
            return Objects.equals(tableIdentifier, that.tableIdentifier)
                    && Objects.equals(columnName, that.columnName)
                    && Objects.equals(subPath, that.subPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableIdentifier, columnName, subPath);
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

        @Override
        public String toString() {
            return "MappedSlot{" + "slot=" + slot + '}';
        }
    }
}
