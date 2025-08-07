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

import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.ParsedPath;

import com.google.common.io.CountingOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * DelegateOutputFile is an implementation of the Iceberg OutputFile interface.
 * It wraps a DorisOutputFile and delegates file output operations to it, providing
 * integration between Doris file system and Iceberg's file IO abstraction.
 */
public class DelegateOutputFile implements OutputFile {
    /**
     * The underlying Doris file system used for file operations.
     */
    private final FileSystem fileSystem;
    /**
     * The DorisOutputFile instance representing the output file.
     */
    private final DorisOutputFile outputFile;

    /**
     * Constructs a DelegateOutputFile with the specified FileSystem and ParsedPath.
     *
     * @param fileSystem the Doris file system to delegate operations to
     * @param path the ParsedPath representing the file location
     */
    public DelegateOutputFile(FileSystem fileSystem, ParsedPath path) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem is null");
        this.outputFile = fileSystem.newOutputFile(path);
    }

    // ===================== File Creation Methods =====================

    /**
     * Creates a new file for writing. Throws UncheckedIOException if creation fails.
     *
     * @return a PositionOutputStream for writing to the file
     */
    @Override
    public PositionOutputStream create() {
        try {
            return new CountingPositionOutputStream(outputFile.create());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + location(), e);
        }
    }

    /**
     * Creates or overwrites a file for writing. Throws UncheckedIOException if creation fails.
     *
     * @return a PositionOutputStream for writing to the file
     */
    @Override
    public PositionOutputStream createOrOverwrite() {
        try {
            return new CountingPositionOutputStream(outputFile.createOrOverwrite());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + location(), e);
        }
    }

    // ===================== File Information Methods =====================

    /**
     * Returns the location (path) of the file as a string.
     *
     * @return the file location
     */
    @Override
    public String location() {
        return outputFile.path().toString();
    }

    /**
     * Converts this output file to an InputFile for reading.
     *
     * @return an InputFile instance for the same file
     */
    @Override
    public InputFile toInputFile() {
        return new DelegateInputFile(fileSystem.newInputFile(outputFile.path()));
    }

    // ===================== Object Methods =====================

    /**
     * Returns a string representation of this DelegateOutputFile.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return outputFile.toString();
    }

    /**
     * CountingPositionOutputStream is a wrapper around OutputStream that tracks the number of bytes written.
     * It extends PositionOutputStream to provide position tracking for Iceberg.
     */
    private static class CountingPositionOutputStream extends PositionOutputStream {
        /**
         * The underlying CountingOutputStream that wraps the actual OutputStream.
         */
        private final CountingOutputStream stream;

        /**
         * Constructs a CountingPositionOutputStream with the specified OutputStream.
         *
         * @param stream the OutputStream to wrap
         */
        private CountingPositionOutputStream(OutputStream stream) {
            this.stream = new CountingOutputStream(stream);
        }

        /**
         * Returns the current position (number of bytes written).
         *
         * @return the number of bytes written
         */
        @Override
        public long getPos() {
            return stream.getCount();
        }

        /**
         * Writes a single byte to the output stream.
         *
         * @param b the byte to write
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void write(int b) throws IOException {
            stream.write(b);
        }

        /**
         * Writes a portion of a byte array to the output stream.
         *
         * @param b the byte array
         * @param off the start offset
         * @param len the number of bytes to write
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        /**
         * Flushes the output stream.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        /**
         * Closes the output stream.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            stream.close();
        }
    }
}
