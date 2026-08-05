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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.mvcc.MvccSnapshot;

import java.util.Objects;

/** A statement-scoped Lance dataset snapshot used by both schema binding and scan planning. */
public class LanceMvccSnapshot implements MvccSnapshot {
    private final LanceTableMetadata metadata;

    public LanceMvccSnapshot(LanceTableMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    }

    public LanceTableMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isSameSnapshot(MvccSnapshot other) {
        if (!(other instanceof LanceMvccSnapshot)) {
            return false;
        }
        LanceTableMetadata otherMetadata = ((LanceMvccSnapshot) other).metadata;
        return metadata.getVersion() == otherMetadata.getVersion()
                && metadata.getDatasetUri().equals(otherMetadata.getDatasetUri());
    }
}
