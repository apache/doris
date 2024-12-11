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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;

import com.google.common.base.Objects;

public class PaimonSchemaCacheKey extends SchemaCacheKey {
    private final long schemaId;

    public PaimonSchemaCacheKey(String dbName, String tableName, long schemaId) {
        super(dbName, tableName);
        this.schemaId = schemaId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonSchemaCacheKey)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PaimonSchemaCacheKey that = (PaimonSchemaCacheKey) o;
        return schemaId == that.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), schemaId);
    }
}
