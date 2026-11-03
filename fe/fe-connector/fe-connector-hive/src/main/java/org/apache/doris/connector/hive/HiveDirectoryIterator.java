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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import org.apache.hadoop.fs.UnsupportedFileSystemException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/** Literal, lazy depth-first traversal. The borrowed filesystem is never closed here. */
final class HiveDirectoryIterator implements HiveFileIterator {
    private final String location;
    private final FileSystem fs;
    private final boolean recursive;
    private final ConnectorScanContext context;
    private final Deque<FileIterator> directories = new ArrayDeque<>();
    private boolean started;
    private boolean closed;
    private HiveFileStatus next;

    HiveDirectoryIterator(String location, FileSystem fs, boolean recursive, ConnectorScanContext context) {
        context.checkCancelled();
        if (fs == null) {
            throw new DorisConnectorException("No filesystem configured for " + location);
        }
        this.location = location;
        this.recursive = recursive;
        this.context = context;
        try {
            this.fs = fs.forLocation(Location.of(location));
        } catch (IOException | RuntimeException e) {
            throw new DorisConnectorException("Failed to resolve filesystem for " + location, e);
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        try {
            context.checkCancelled();
            if (next != null) {
                return true;
            }
            if (!started) {
                started = true;
                openDirectory(Location.of(location));
            }
            while (!directories.isEmpty()) {
                context.checkCancelled();
                FileIterator files = directories.peek();
                if (!files.hasNext()) {
                    directories.pop().close();
                    continue;
                }
                FileEntry entry = files.next();
                String name = entry.name();
                if (name.startsWith("_") || name.startsWith(".")) {
                    continue;
                }
                if (entry.isDirectory()) {
                    if (recursive) {
                        openDirectory(entry.location());
                    }
                    continue;
                }
                next = new HiveFileStatus(entry.location().uri(), entry.length(), entry.modificationTime());
                return true;
            }
            closed = true;
            return false;
        } catch (IOException e) {
            RuntimeException failure = listingFailure(e);
            closeAfterFailure(failure);
            throw failure;
        } catch (RuntimeException | Error e) {
            closeAfterFailure(e);
            throw e;
        }
    }

    private void openDirectory(Location dir) throws IOException {
        context.checkCancelled();
        directories.push(fs.list(dir).withCancellationCheck(context::checkCancelled));
    }

    @Override
    public HiveFileStatus next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        HiveFileStatus result = next;
        next = null;
        return result;
    }

    @Override
    public void close() {
        closed = true;
        next = null;
        Throwable failure = null;
        while (!directories.isEmpty()) {
            try {
                directories.pop().close();
            } catch (IOException | RuntimeException e) {
                if (failure == null) {
                    failure = e;
                    continue;
                }
                failure.addSuppressed(e);
            }
        }
        if (failure instanceof IOException) {
            throw listingFailure((IOException) failure);
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
    }

    private void closeAfterFailure(Throwable failure) {
        try {
            close();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    private RuntimeException listingFailure(IOException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (cause instanceof UnsupportedFileSystemException
                    || message != null && message.contains("No FileSystem for scheme")) {
                return new DorisConnectorException("Failed to resolve filesystem for " + location, failure);
            }
        }
        return new HiveDirectoryListingException("Failed to list files under " + location, failure);
    }
}
