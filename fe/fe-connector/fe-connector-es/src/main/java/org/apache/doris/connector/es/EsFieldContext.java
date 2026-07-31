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

package org.apache.doris.connector.es;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds field-level context derived from ES index mapping analysis.
 *
 * <ul>
 *   <li>{@code fetchFieldsContext} — maps text fields to their keyword sub-fields
 *       (e.g. "city" → "city.raw"), used when {@code enable_keyword_sniff = true}.</li>
 *   <li>{@code docValueFieldsContext} — maps columns to doc_value-capable field paths,
 *       used when {@code enable_docvalue_scan = true}.</li>
 *   <li>{@code needCompatDateFields} — columns using default/strict_date_optional_time format
 *       that need special date compatibility handling.</li>
 * </ul>
 */
public class EsFieldContext {

    private final Map<String, String> fetchFieldsContext = new HashMap<>();
    private final Map<String, String> docValueFieldsContext = new HashMap<>();
    private final List<String> needCompatDateFields = new ArrayList<>();
    private final Map<String, String> column2typeMap = new HashMap<>();

    public Map<String, String> getFetchFieldsContext() {
        return fetchFieldsContext;
    }

    public Map<String, String> getDocValueFieldsContext() {
        return docValueFieldsContext;
    }

    public List<String> getNeedCompatDateFields() {
        return needCompatDateFields;
    }

    /**
     * Returns column name → ES raw type string mapping (e.g., "keyword", "text", "long").
     * Used by query DSL builder to determine whether LIKE pushdown is safe on keyword fields.
     */
    public Map<String, String> getColumn2typeMap() {
        return column2typeMap;
    }

    @Override
    public String toString() {
        return "EsFieldContext{"
                + "fetchFields=" + fetchFieldsContext.size()
                + ", docValueFields=" + docValueFieldsContext.size()
                + ", compatDateFields=" + needCompatDateFields.size()
                + ", column2type=" + column2typeMap.size()
                + '}';
    }
}
