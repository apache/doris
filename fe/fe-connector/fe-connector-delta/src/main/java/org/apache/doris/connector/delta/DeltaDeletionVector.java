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

package org.apache.doris.connector.delta;

import java.util.Objects;
import java.util.Optional;

/** Immutable Delta deletion-vector descriptor carried from Kernel to the BE scanner. */
public final class DeltaDeletionVector {
    private final String storageType;
    private final String pathOrInlineDv;
    private final Optional<Integer> offset;
    private final int sizeInBytes;
    private final long cardinality;

    public DeltaDeletionVector(String storageType, String pathOrInlineDv,
            Optional<Integer> offset, int sizeInBytes, long cardinality) {
        this.storageType = Objects.requireNonNull(storageType, "storageType");
        this.pathOrInlineDv = Objects.requireNonNull(pathOrInlineDv, "pathOrInlineDv");
        this.offset = Objects.requireNonNull(offset, "offset");
        this.sizeInBytes = sizeInBytes;
        this.cardinality = cardinality;
    }

    public String getStorageType() {
        return storageType;
    }

    public String getPathOrInlineDv() {
        return pathOrInlineDv;
    }

    public Optional<Integer> getOffset() {
        return offset;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public long getCardinality() {
        return cardinality;
    }
}
