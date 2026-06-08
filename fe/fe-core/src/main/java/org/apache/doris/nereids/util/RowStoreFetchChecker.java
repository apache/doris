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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Utility to determine whether FE can safely request row-store lazy fetch on BE. */
public final class RowStoreFetchChecker {
    private RowStoreFetchChecker() {
    }

    /**
     * Check if we can use row-store lazy fetch for the given lazy slots. The check is based on the following criteria:
     */
    public static boolean canUseRowStoreForLazySlots(List<Slot> lazySlots) {
        Set<Integer> originalColumnUniqueIds = new HashSet<>();
        for (Slot lazySlot : lazySlots) {
            if (!(lazySlot instanceof SlotReference)) {
                return false;
            }
            SlotReference slotReference = (SlotReference) lazySlot;
            // BE row-store fetch maps values only by col_unique_id and does not apply sub-column
            // paths or nested-column pruning access paths.
            if (slotReference.hasSubColPath() || hasNestedAccessPaths(slotReference)) {
                return false;
            }
            Optional<Column> originalColumn = slotReference.getOriginalColumn();
            if (!originalColumn.isPresent() || originalColumn.get().getType().isComplexType()
                    || !originalColumnUniqueIds.add(originalColumn.get().getUniqueId())) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNestedAccessPaths(SlotReference slotReference) {
        return slotReference.getAllAccessPaths().map(paths -> !paths.isEmpty()).orElse(false)
                || slotReference.getPredicateAccessPaths().map(paths -> !paths.isEmpty()).orElse(false);
    }
}
