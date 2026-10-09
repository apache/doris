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

package org.apache.doris.paimon;

import org.apache.paimon.CoreOptions;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The columns whose element ROW must not be narrowed by nested-column pruning.
 *
 * <p>A primary-key table with an {@code ARRAY<ROW>} column can bind that column to the
 * {@code nested_update} aggregate function ({@code fields.<col>.aggregate-function = nested_update}).
 * The merge engine then builds a key projection against the FULL element row type and uses it to match
 * rows while merging sorted runs ({@code FieldNestedUpdateAgg}). That projection is generated from the
 * column's declared {@code Array<Row>} type, not from whatever shape a reader hands back, so it indexes
 * the element fields by their ORIGINAL positions.
 *
 * <p>Doris' nested-column pruning rewrites the read type to only the sub-fields a query touches and
 * pushes it down with {@code ReadBuilder.withReadType}. When that drops the field named by
 * {@code fields.<col>.nested-key} (the first element field), the merge engine still projects that
 * position out of the narrower rows and reads whatever now sits there — a
 * {@code ClassCastException} (e.g. {@code HeapBytesVector} as {@code LongColumnVector}) that only
 * appears for splits that need merge-on-read, i.e. several sorted runs for one key.
 *
 * <p>So for such a column the whole element row must be read: the pruned shape is not a safe input to
 * the merge engine. This is a property of the column's declared aggregate function, so it is resolved
 * from the table options once per scan and looked up per projected column by name.
 */
final class PaimonNestedUpdateColumns {

    private static final String NESTED_UPDATE = "nested_update";

    private final Map<String, List<String>> requiredNestedKeys;

    private PaimonNestedUpdateColumns(Map<String, List<String>> requiredNestedKeys) {
        this.requiredNestedKeys = requiredNestedKeys;
    }

    /** Resolves the {@code nested_update} columns of a table from its merged option map. */
    static PaimonNestedUpdateColumns resolve(Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return new PaimonNestedUpdateColumns(Collections.emptyMap());
        }
        CoreOptions options = CoreOptions.fromMap(tableOptions);
        Map<String, List<String>> keys = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String optionKey = entry.getKey();
            if (!optionKey.startsWith(CoreOptions.FIELDS_PREFIX + ".")) {
                continue;
            }
            // fields.<column>.aggregate-function — <column> may itself contain dots in a quoted name,
            // so strip the fixed prefix and suffix instead of splitting on '.'.
            String suffix = "." + CoreOptions.AGG_FUNCTION;
            if (!optionKey.endsWith(suffix)) {
                continue;
            }
            String column =
                    optionKey.substring(CoreOptions.FIELDS_PREFIX.length() + 1,
                            optionKey.length() - suffix.length());
            if (!NESTED_UPDATE.equalsIgnoreCase(entry.getValue())) {
                continue;
            }
            // A nested_update column without a nested-key is legal (full-element dedup); there is no
            // field whose pruning would corrupt the merge, so it needs no pinning.
            List<String> nestedKey = options.fieldNestedUpdateAggNestedKey(column);
            if (!nestedKey.isEmpty()) {
                keys.put(column.toLowerCase(Locale.ROOT), nestedKey);
            }
        }
        return new PaimonNestedUpdateColumns(Collections.unmodifiableMap(keys));
    }

    /**
     * @return the element-ROW field names the merge engine reads by position for {@code column}, or an
     *         empty list when the column is not a {@code nested_update} column. A non-empty answer
     *         means the column's element row must be read in full.
     */
    List<String> requiredElementFields(String column) {
        if (column == null) {
            return Collections.emptyList();
        }
        List<String> keys = requiredNestedKeys.get(column.toLowerCase(Locale.ROOT));
        return keys == null ? Collections.emptyList() : keys;
    }

    boolean isEmpty() {
        return requiredNestedKeys.isEmpty();
    }
}
