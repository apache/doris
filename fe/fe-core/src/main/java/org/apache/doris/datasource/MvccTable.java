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

import org.apache.doris.catalog.TableIf;

public interface MvccTable extends TableIf {
    /**
     * Get the latest snapshotId (obtained from Doris cache, not directly through API)
     *
     * @return snapshotId
     */
    long getLatestSnapshotId();

    /**
     * When obtaining partition and other information based on snapshotId,
     * this method needs to be called first to add a reference to the snapshotId and prevent the cache corresponding
     * to the snapshotId from being deleted
     *
     * @param snapshotId
     */
    void ref(long snapshotId);

    /**
     * Release the reference of the snapshotId.
     * It is paired with 'ref'.
     * You need to call this method in 'finally' to prevent the reference from not being released,
     * causing the cache of the snapshotId to remain
     *
     * @param snapshotId
     */
    void unref(long snapshotId);
}
