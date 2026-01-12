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

package org.apache.doris.datasource.iceberg.fileio;

import org.apache.doris.fs.io.DorisInputFile;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * DelegateInputFile is an implementation of the Iceberg InputFile interface.
 * It wraps a DorisInputFile and delegates file input operations to it, providing
 * integration between Doris file system and Iceberg's file IO abstraction.
 */
public class DelegateInputFile implements InputFile {
    /**
     * The underlying DorisInputFile used for file operations.
     */
    private final DorisInputFile inputFile;

    /**
     * Constructs a DelegateInputFile with the specified DorisInputFile.
     * @param inputFile the DorisInputFile to delegate operations to
     */
    public DelegateInputFile(DorisInputFile inputFile) {
        this.inputFile = Objects.requireNonNull(inputFile, "inputFile is null");
    }

    // ===================== File Information Methods =====================

    /**
     * Returns the length of the file in bytes.
     * Throws UncheckedIOException if the file status cannot be retrieved.
     * @return the file length in bytes
     */
    @Override
    public long getLength() {
        try {
            return inputFile.length();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get status for file: " + location(), e);
        }
    }

    /**
     * Returns the location (path) of the file as a string.
     * @return the file location
     */
    @Override
    public String location() {
        return inputFile.path().toString();
    }

    /**
     * Checks if the file exists.
     * Throws UncheckedIOException if the existence check fails.
     * @return true if the file exists, false otherwise
     */
    @Override
    public boolean exists() {
        try {
            return inputFile.exists();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to check existence for file: " + location(), e);
        }
    }

    // ===================== File Stream Methods =====================

    /**
     * Opens a new SeekableInputStream for reading the file.
     * Throws NotFoundException if the file is not found, or UncheckedIOException for other IO errors.
     * @return a SeekableInputStream for the file
     */
    @Override
    public SeekableInputStream newStream() {
        try {
            return new DelegateSeekableInputStream(inputFile.newStream());
        } catch (FileNotFoundException e) {
            throw new NotFoundException(e, "Failed to open input stream for file: %s", location());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open input stream for file: " + location(), e);
        }
    }

    // ===================== Object Methods =====================

    /**
     * Returns a string representation of this DelegateInputFile.
     * @return string representation
     */
    @Override
    public String toString() {
        return inputFile.toString();
    }
}
