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

package org.apache.doris.datasource.lance.metadata;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Names and field IDs for prewarm, independent of planner-only index descriptions. */
public final class LanceIndexPrewarmMetadata {
    private final LanceTableAccess access;
    private final long version;
    private final Schema schema;
    private final Map<String, Integer> fieldIds;
    private final Map<String, List<Integer>> indexFields;

    public LanceIndexPrewarmMetadata(LanceTableAccess access, long version, Schema schema,
            Map<String, Integer> fieldIds, Map<String, List<Integer>> indexFields) {
        this.access = access;
        this.version = version;
        this.schema = schema;
        this.fieldIds = Collections.unmodifiableMap(new HashMap<>(fieldIds));
        Map<String, List<Integer>> copied = new LinkedHashMap<>();
        indexFields.forEach((name, fields) -> copied.put(name,
                Collections.unmodifiableList(new ArrayList<>(fields))));
        this.indexFields = Collections.unmodifiableMap(copied);
    }

    public String getDatasetUri() {
        return access.getDatasetUri();
    }

    public long getVersion() {
        return version;
    }

    public Schema getSchema() {
        return schema;
    }

    public OptionalInt getLanceFieldId(String fieldName) {
        Integer id = fieldIds.get(fieldName);
        return id == null ? OptionalInt.empty() : OptionalInt.of(id);
    }

    public Map<String, List<Integer>> getIndexFields() {
        return indexFields;
    }

    public Map<String, String> getLanceStorageOptions() {
        return access.getStorageOptions();
    }
}
