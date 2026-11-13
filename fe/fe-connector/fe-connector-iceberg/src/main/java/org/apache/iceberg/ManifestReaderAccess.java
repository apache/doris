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

import org.apache.iceberg.io.CloseableIterable;

/** Package bridge for filtering borrowed manifest entries before copying retained files. */
public final class ManifestReaderAccess {
    private ManifestReaderAccess() {
    }

    /**
     * The reader owns all underlying streams and must remain open during iteration. Files are reused;
     * callers must finish inspecting each file and copy any retained value before advancing.
     */
    public static <F extends ContentFile<F>> Iterable<F> liveFiles(ManifestReader<F> reader) {
        return CloseableIterable.transform(reader.liveEntries(), ManifestEntry::file);
    }
}
