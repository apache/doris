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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/** Fail before planning files when a table requires semantics the native reader has not validated. */
final class DeltaLakeFeatureCheck {
    // Features are enabled alongside their end-to-end read coverage.
    static final Set<String> SUPPORTED_READER_FEATURES = Collections.emptySet();

    private DeltaLakeFeatureCheck() {
    }

    static void check(Snapshot snapshot) {
        check(snapshot.getPath(), DeltaKernelCompat.readerFeatures(snapshot));
    }

    static void check(String table, Set<String> readerFeatures) {
        Set<String> unsupported = new TreeSet<>(readerFeatures);
        unsupported.removeAll(SUPPORTED_READER_FEATURES);
        if (!unsupported.isEmpty()) {
            throw new DorisConnectorException("Unsupported Delta Lake reader features " + unsupported
                    + " for table " + table);
        }
    }
}
