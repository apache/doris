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

import org.apache.doris.datasource.NameMapping;

import java.util.Objects;

/** Stable identity for a Paimon projection hydrated from one captured snapshot/schema fence. */
public final class PaimonSnapshotEntryKey {
    private final NameMapping nameMapping;
    private final long snapshotId;
    private final long schemaId;
    private final long tableGeneration;

    public PaimonSnapshotEntryKey(
            NameMapping nameMapping, long snapshotId, long schemaId, long tableGeneration) {
        this.nameMapping = Objects.requireNonNull(nameMapping, "nameMapping can not be null");
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.tableGeneration = tableGeneration;
    }

    public static PaimonSnapshotEntryKey of(
            NameMapping nameMapping, PaimonSnapshot fence, long tableGeneration) {
        return new PaimonSnapshotEntryKey(
                nameMapping, fence.getSnapshotId(), fence.getSchemaId(), tableGeneration);
    }

    public NameMapping getNameMapping() {
        return nameMapping;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public long getTableGeneration() {
        return tableGeneration;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PaimonSnapshotEntryKey)) {
            return false;
        }
        PaimonSnapshotEntryKey that = (PaimonSnapshotEntryKey) object;
        return snapshotId == that.snapshotId
                && schemaId == that.schemaId
                && tableGeneration == that.tableGeneration
                && nameMapping.equals(that.nameMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameMapping, snapshotId, schemaId, tableGeneration);
    }
}
