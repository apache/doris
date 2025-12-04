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

package org.apache.doris.datasource.hudi;

import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.NameMapping;

import com.google.common.base.Objects;

/**
 * Cache key for Hudi table schemas that includes timestamp information.
 * This allows for time-travel queries and ensures proper schema versioning.
 */
public class HudiSchemaCacheKey extends SchemaCacheKey {
    private final long timestamp;

    /**
     * Creates a new cache key for Hudi table schemas.
     *
     * @param nameMapping
     * @param timestamp The timestamp for schema version
     * @throws IllegalArgumentException if dbName or tableName is null or empty
     */
    public HudiSchemaCacheKey(NameMapping nameMapping, long timestamp) {
        super(nameMapping);
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative");
        }
        this.timestamp = timestamp;
    }

    /**
     * Gets the timestamp associated with this schema version.
     *
     * @return the timestamp value
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        HudiSchemaCacheKey that = (HudiSchemaCacheKey) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), timestamp);
    }

    @Override
    public String toString() {
        return String.format("HudiSchemaCacheKey{dbName='%s', tableName='%s', timestamp=%d}",
                getNameMapping().getLocalDbName(), getNameMapping().getLocalTblName(), timestamp);
    }
}
