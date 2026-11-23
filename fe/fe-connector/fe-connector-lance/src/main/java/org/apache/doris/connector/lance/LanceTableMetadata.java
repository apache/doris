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

package org.apache.doris.connector.lance;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable metadata resolved from one already-fixed Lance dataset version. */
final class LanceTableMetadata {
    private final LanceTableAccess access;
    private final long version;
    private final Schema schema;
    private final List<LanceFragmentInfo> fragments;

    LanceTableMetadata(LanceTableAccess access, long version, Schema schema, List<LanceFragmentInfo> fragments) {
        this.access = Objects.requireNonNull(access, "access");
        this.version = version;
        this.schema = Objects.requireNonNull(schema, "schema");
        this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
    }

    String getDatasetUri() {
        return access.getDatasetUri();
    }

    long getVersion() {
        return version;
    }

    Schema getSchema() {
        return schema;
    }

    /** The fragments of this version in manifest order. */
    List<LanceFragmentInfo> getFragments() {
        return fragments;
    }

    /** Lance object-store options, understood as-is by both the FE SDK and lance-c. */
    Map<String, String> getLanceStorageOptions() {
        return access.getStorageOptions();
    }

    /** The logical row count of this version: physical rows minus deleted ones. */
    long getRowCount() {
        return fragments.stream().mapToLong(LanceFragmentInfo::getRowCount).sum();
    }
}
