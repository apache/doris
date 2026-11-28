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

package org.apache.iceberg;

import org.apache.iceberg.io.FileIO;

/** Accesses the FileIO owned by Iceberg's serialized metadata tasks. */
public final class DorisMetadataTaskFileIO {
    private DorisMetadataTaskFileIO() {
    }

    /** Returns the task's top-level owner, or null for tasks without an embedded FileIO. */
    public static FileIO fileIO(FileScanTask task) {
        // These accessors are package-private. This bridge must share Iceberg's defining loader,
        // and returns only the top-level owner so nested resolver delegates are not closed separately.
        if (task instanceof BaseFilesTable.ManifestReadTask) {
            return ((BaseFilesTable.ManifestReadTask) task).io();
        }
        if (task instanceof BaseEntriesTable.ManifestReadTask) {
            return ((BaseEntriesTable.ManifestReadTask) task).io();
        }
        if (task instanceof AllManifestsTable.ManifestListReadTask) {
            return ((AllManifestsTable.ManifestListReadTask) task).io();
        }
        // StaticDataTask holds materialized rows and has no FileIO to release.
        return null;
    }
}
