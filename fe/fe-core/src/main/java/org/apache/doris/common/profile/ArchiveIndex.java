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

package org.apache.doris.common.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ArchiveIndex provides in-memory indexing functionality for archived profiles.
 * It maintains metadata about archived profiles for fast lookup and querying.
 */
public class ArchiveIndex {
    private static final Logger LOG = LogManager.getLogger(ArchiveIndex.class);

    /**
     * Metadata for an archived profile entry.
     */
    public static class ArchiveEntry {
        private final String queryId;
        private final String archivePath;
        private final long queryFinishTime;
        private final long fileSize;

        public ArchiveEntry(String queryId, String archivePath, long queryFinishTime,
                          long fileSize) {
            this.queryId = queryId;
            this.archivePath = archivePath;
            this.queryFinishTime = queryFinishTime;
            this.fileSize = fileSize;
        }

        public String getQueryId() {
            return queryId;
        }

        public String getArchivePath() {
            return archivePath;
        }

        public long getQueryFinishTime() {
            return queryFinishTime;
        }

        public long getFileSize() {
            return fileSize;
        }

        @Override
        public String toString() {
            return String.format("ArchiveEntry{queryId='%s', archivePath='%s', queryFinishTime=%d, fileSize=%d}",
                    queryId, archivePath, queryFinishTime, fileSize);
        }
    }

    // Index by query ID for fast lookup
    private final Map<String, ArchiveEntry> queryIdIndex = new ConcurrentHashMap<>();

    /**
     * Add an archive entry to the index.
     *
     * @param entry the archive entry to add
     */
    public void addEntry(ArchiveEntry entry) {
        // ConcurrentHashMap.put() is thread-safe and atomic
        queryIdIndex.put(entry.getQueryId(), entry);
    }

    /**
     * Remove an archive entry from the index.
     *
     * @param queryId the query ID of the entry to remove
     * @return the removed entry, or null if not found
     */
    public ArchiveEntry removeEntry(String queryId) {
        // ConcurrentHashMap.remove() is thread-safe and atomic
        ArchiveEntry entry = queryIdIndex.remove(queryId);
        return entry;
    }

    /**
     * Get an archive entry by query ID.
     *
     * @param queryId the query ID
     * @return the archive entry, or null if not found
     */
    public ArchiveEntry getEntry(String queryId) {
        // ConcurrentHashMap.get() is thread-safe and lock-free
        return queryIdIndex.get(queryId);
    }


    /**
     * Get all archive entries.
     *
     * @return list of all archive entries
     */
    public List<ArchiveEntry> getAllEntries() {
        // ConcurrentHashMap.values() provides a consistent snapshot
        // No explicit locking needed as we create a new ArrayList
        return new ArrayList<>(queryIdIndex.values());
    }

    /**
     * Get the total number of archived entries.
     *
     * @return the total count
     */
    public int getEntryCount() {
        // ConcurrentHashMap.size() is thread-safe and atomic
        return queryIdIndex.size();
    }

    /**
     * Get the total size of all archived files.
     *
     * @return total size in bytes
     */
    public long getTotalSize() {
        // Stream operations on ConcurrentHashMap.values() are weakly consistent
        // They reflect the state at some point during the operation
        return queryIdIndex.values().stream()
                .mapToLong(ArchiveEntry::getFileSize)
                .sum();
    }

    /**
     * Check if an entry exists for the given query ID.
     *
     * @param queryId the query ID
     * @return true if the entry exists
     */
    public boolean containsEntry(String queryId) {
        // ConcurrentHashMap.containsKey() is thread-safe and lock-free
        return queryIdIndex.containsKey(queryId);
    }


    /**
     * Get entries older than the specified time.
     *
     * @param cutoffTimestamp the cutoff time
     * @return list of entries older than the cutoff time
     */
    public List<ArchiveEntry> getEntriesOlderThan(long cutoffTimestamp) {
        return queryIdIndex.values().stream()
                .filter(entry -> entry.getQueryFinishTime() < cutoffTimestamp)
                .collect(Collectors.toList());
    }
}
