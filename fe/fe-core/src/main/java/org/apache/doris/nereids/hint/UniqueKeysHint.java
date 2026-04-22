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

package org.apache.doris.nereids.hint;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * UNIQUE_KEYS hint for table-level uniqueness declaration.
 */
public class UniqueKeysHint extends Hint {
    private static final Logger LOG = LoggerFactory.getLogger(UniqueKeysHint.class);

    private final String originalString;
    // normalized table name -> list of unique key sets
    private final Map<String, List<List<String>>> uniqueKeysByTable;

    /**
     * Constructor for UNIQUE_KEYS runtime hint.
     */
    public UniqueKeysHint(String hintName, Map<String, List<List<String>>> uniqueKeysByTable, String originalString) {
        super(hintName);
        this.originalString = originalString;
        this.uniqueKeysByTable = uniqueKeysByTable;
    }

    /**
     * Add unique slot sets for the table if there are applicable UNIQUE_KEYS definitions.
     */
    public void addUniqueSlotsIfMatched(TableIf table, String qualifiedTableName,
            Set<Slot> outputSet, DataTrait.Builder builder) {
        List<List<String>> keySets = uniqueKeysByTable.get(normalizeQualifiedName(qualifiedTableName));
        if (keySets == null || keySets.isEmpty()) {
            return;
        }

        Map<String, SlotReference> slotByColumnName = new HashMap<>();
        for (Slot slot : outputSet) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            SlotReference slotRef = (SlotReference) slot;
            if (!slotRef.getOriginalColumn().isPresent()) {
                continue;
            }
            slotByColumnName.put(normalizeIdentifier(slotRef.getOriginalColumn().get().getName()), slotRef);
        }

        for (List<String> keySet : keySets) {
            if (keySet == null || keySet.isEmpty()) {
                LOG.warn("UNIQUE_KEYS has empty unique key definition on table {}", table.getName());
                break;
            }
            ImmutableSet.Builder<Slot> uniqueSlots = ImmutableSet.builder();
            boolean matched = true;
            for (String column : keySet) {
                SlotReference slot = slotByColumnName.get(normalizeIdentifier(column));
                if (slot == null) {
                    LOG.warn("UNIQUE_KEYS references unknown column '{}' on table {}", column, table.getName());
                    matched = false;
                    break;
                }
                uniqueSlots.add(slot);
            }
            if (matched) {
                ImmutableSet<Slot> slotSet = uniqueSlots.build();
                if (slotSet.isEmpty()) {
                    LOG.warn("UNIQUE_KEYS resolved empty unique slot set on table {}", table.getName());
                    break;
                }
                builder.addUniqueSlot(slotSet);
            }
        }
        setStatus(HintStatus.SUCCESS);
    }

    @Override
    public String getExplainString() {
        return originalString;
    }

    public static boolean isQualifiedName(String name) {
        return !name.isEmpty() && name.split("\\.").length == 3;
    }

    /**
     * Normalize a fully qualified table name to canonical form.
     *
     * <p>Expected format is {@code catalog.db.table}. Each segment is trimmed,
     * optional backticks are removed, and the result is lower-cased.
     * If the input is null, malformed, or any segment becomes empty after
     * normalization, this method returns an empty string.
     *
     * @param name table name in fully qualified form
     * @return normalized qualified name, or empty string if invalid
     */
    public static String normalizeQualifiedName(String name) {
        if (name == null) {
            return "";
        }
        String[] parts = name.trim().split("\\.");
        if (parts.length != 3) {
            return "";
        }
        String[] normalizedParts = new String[3];
        for (int i = 0; i < parts.length; i++) {
            normalizedParts[i] = normalizeIdentifier(parts[i]);
            if (normalizedParts[i].isEmpty()) {
                return "";
            }
        }
        return String.join(".", normalizedParts);
    }

    private static String normalizeIdentifier(String name) {
        String normalized = name == null ? "" : name.trim();
        if (normalized.startsWith("`") && normalized.endsWith("`") && normalized.length() > 1) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return normalized.toLowerCase(Locale.ROOT);
    }
}

