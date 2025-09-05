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
import java.util.StringJoiner;

public class PaimonSnapshotCacheKey {
    private final NameMapping nameMapping;
    private final long snapshotId;

    public PaimonSnapshotCacheKey(NameMapping nameMapping, long snapshotId) {
        this.nameMapping = nameMapping;
        this.snapshotId = snapshotId;
    }

    public NameMapping getNameMapping() {
        return nameMapping;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonSnapshotCacheKey that = (PaimonSnapshotCacheKey) o;
        return snapshotId == that.snapshotId && Objects.equals(nameMapping, that.nameMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameMapping, snapshotId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PaimonSnapshotCacheKey.class.getSimpleName() + "[", "]")
                .add("catalog=" + nameMapping.getCtlId())
                .add("dbName='" + nameMapping.getLocalDbName() + "'")
                .add("tableName='" + nameMapping.getLocalTblName() + "'")
                .add("snapshotId='" + snapshotId + "'")
                .toString();
    }
}
