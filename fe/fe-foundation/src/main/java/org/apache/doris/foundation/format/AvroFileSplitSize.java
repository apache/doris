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

package org.apache.doris.foundation.format;

/** Bounds repeated OCF prefix scans until readers can reuse block positions between ranges. */
public final class AvroFileSplitSize {
    public static final int MAX_SPLITS = 2;

    private AvroFileSplitSize() {
    }

    public static long bounded(long fileSize, long targetSize) {
        // Every nonzero start walks from the header. For zero-based byte ranges, this keeps
        // repeated prefix traversal below one file, rather than 7.5 files for sixteen starts.
        return Math.max(targetSize, fileSize / MAX_SPLITS + (fileSize % MAX_SPLITS == 0 ? 0 : 1));
    }
}
