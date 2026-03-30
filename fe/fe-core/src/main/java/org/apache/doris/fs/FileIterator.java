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

package org.apache.doris.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Lazy iterator over {@link FileEntry} objects in a directory listing.
 * <p>
 * Unlike {@link java.util.Iterator}, both {@link #hasNext()} and {@link #next()} may throw
 * {@link IOException} since they may perform remote I/O (e.g., pagination requests).
 * <p>
 * Implementations must be {@link Closeable}. Always use in a try-with-resources block:
 * <pre>
 *   try (FileIterator it = fs.listFiles(location, false)) {
 *       while (it.hasNext()) {
 *           FileEntry entry = it.next();
 *           // process entry
 *       }
 *   }
 * </pre>
 */
public interface FileIterator extends Closeable {

    /**
     * Returns {@code true} if there are more entries to iterate.
     *
     * @throws IOException if an I/O error occurs while checking for more entries
     */
    boolean hasNext() throws IOException;

    /**
     * Returns the next {@link FileEntry}.
     *
     * @throws IOException            if an I/O error occurs while fetching the next entry
     * @throws NoSuchElementException if there are no more entries
     */
    FileEntry next() throws IOException;

    /**
     * Releases any resources held by this iterator (e.g., HTTP connections).
     * Idempotent — safe to call multiple times.
     */
    @Override
    void close() throws IOException;

    /**
     * Drains the remaining entries into a list.
     * Consumes the iterator; calling {@link #hasNext()} afterwards returns {@code false}.
     */
    default List<FileEntry> toList() throws IOException {
        List<FileEntry> result = new ArrayList<>();
        while (hasNext()) {
            result.add(next());
        }
        return result;
    }

    /** Returns an empty iterator. */
    static FileIterator empty() {
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public FileEntry next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
            }
        };
    }

    /** Wraps a pre-built list as a FileIterator. Useful for testing and adapters. */
    static FileIterator ofList(List<FileEntry> entries) {
        return new FileIterator() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < entries.size();
            }

            @Override
            public FileEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return entries.get(index++);
            }

            @Override
            public void close() {
            }
        };
    }
}
