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

package org.apache.doris.filesystem;

import java.util.List;

/**
 * Result of a {@link FileSystem#globListWithLimit} operation.
 *
 * <p>Carries the matching file list plus the S3 bucket, prefix, and the maximum file key
 * (the last key seen in the full listing, possibly beyond the returned page).
 * {@code maxFile} allows callers to determine whether more files are available after the
 * returned page without issuing an additional listing request.
 */
public final class GlobListing {

    private final List<FileEntry> files;
    /** S3 bucket name extracted from the listing URI. */
    private final String bucket;
    /** Key prefix used for the listing (longest non-glob prefix of the path pattern). */
    private final String prefix;
    /**
     * The key of the last matching object observed in the listing, including objects
     * beyond the page limit. Empty string if no objects were observed at all.
     */
    private final String maxFile;

    public GlobListing(List<FileEntry> files, String bucket, String prefix, String maxFile) {
        this.files = List.copyOf(files);
        this.bucket = bucket;
        this.prefix = prefix;
        this.maxFile = maxFile;
    }

    /** Returns the list of file entries matching the glob pattern, up to the requested limits. */
    public List<FileEntry> getFiles() {
        return files;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getMaxFile() {
        return maxFile;
    }
}
