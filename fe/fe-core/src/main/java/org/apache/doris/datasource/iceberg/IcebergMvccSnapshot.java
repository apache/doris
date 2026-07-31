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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.mvcc.MvccSnapshot;

public class IcebergMvccSnapshot implements MvccSnapshot {
    private final IcebergSnapshotCacheValue snapshotCacheValue;

    public IcebergMvccSnapshot(IcebergSnapshotCacheValue snapshotCacheValue) {
        this.snapshotCacheValue = snapshotCacheValue;
    }

    public IcebergSnapshotCacheValue getSnapshotCacheValue() {
        return snapshotCacheValue;
    }

    @Override
    public boolean isSameSnapshot(MvccSnapshot other) {
        if (!(other instanceof IcebergMvccSnapshot)) {
            return false;
        }
        IcebergSnapshot left = snapshotCacheValue.getSnapshot();
        IcebergSnapshotCacheValue otherCacheValue = ((IcebergMvccSnapshot) other).snapshotCacheValue;
        IcebergSnapshot right = otherCacheValue.getSnapshot();
        // A branch can retain its data snapshot while adopting a newer current schema.
        // Name mapping is also scan-visible state and may change without a snapshot or schema ID change.
        return left.getSnapshotId() == right.getSnapshotId()
                && left.getSchemaId() == right.getSchemaId()
                && snapshotCacheValue.getNameMapping().equals(otherCacheValue.getNameMapping());
    }
}
