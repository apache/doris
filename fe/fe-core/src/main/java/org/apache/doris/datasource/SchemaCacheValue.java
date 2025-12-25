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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The cache value of ExternalSchemaCache.
 * Different external table type has different schema cache value.
 * For example, Hive table has HMSSchemaCacheValue, Paimon table has PaimonSchemaCacheValue.
 * All objects that should be refreshed along with schema should be put in this class.
 */
public class SchemaCacheValue {
    public static final long DEFAULT_VERSION = -1L;
    protected final Map<Long, List<Column>> schemas;
    protected volatile long primaryVersionId;

    public SchemaCacheValue(List<Column> schema) {
        this(Collections.singletonMap(DEFAULT_VERSION, schema), DEFAULT_VERSION);
    }

    public SchemaCacheValue(Map<Long, List<Column>> schemas, long primaryVersionId) {
        this.schemas = new ConcurrentHashMap<>(schemas);
        this.primaryVersionId = primaryVersionId;
    }

    public Map<Long, List<Column>> getSchemas() {
        return Collections.unmodifiableMap(schemas);
    }

    public List<Column> getSchema() {
        List<Column> primary = schemas.get(primaryVersionId);
        if (primary != null) {
            return primary;
        }
        return schemas.values().stream().findFirst().orElse(null);
    }

    public List<Column> getSchema(long versionId) {
        return schemas.get(versionId);
    }

    public void addSchema(long versionId, List<Column> schema) {
        validateSchema(schema);
        schemas.put(versionId, schema);
    }

    public boolean containsVersion(long versionId) {
        return schemas.containsKey(versionId);
    }

    public void validateSchema() throws IllegalArgumentException {
        for (List<Column> schema : schemas.values()) {
            validateSchema(schema);
        }
    }

    protected void validateSchema(List<Column> schema) {
        Set<String> columnNames = new HashSet<>();
        for (Column column : schema) {
            if (!columnNames.add(column.getName().toLowerCase())) {
                throw new IllegalArgumentException("Duplicate column name found: " + column.getName());
            }
            // Add more validation logic if needed
        }
    }
}
