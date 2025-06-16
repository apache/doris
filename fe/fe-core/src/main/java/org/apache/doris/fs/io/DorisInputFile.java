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

package org.apache.doris.fs.io;

import java.io.IOException;

/**
 * DorisInputFile represents an abstraction for a file input source in Doris filesystem layer.
 * <p>
 * This interface provides methods to access file content, retrieve file metadata, and check file existence.
 * Implementations of this interface allow Doris to interact with various file systems in a unified way.
 */
public interface DorisInputFile {
    /**
     * Returns the path of this file as a ParsedPath object.
     *
     * @return the ParsedPath representing the file location
     */
    ParsedPath path();

    /**
     * Returns the length of the file in bytes.
     *
     * @return the size of the file in bytes
     * @throws IOException if an I/O error occurs
     */
    long length() throws IOException;

    /**
     * Returns the last modified time of the file.
     *
     * @return the last modified timestamp in milliseconds since epoch
     * @throws IOException if an I/O error occurs
     */
    long lastModifiedTime() throws IOException;

    /**
     * Checks whether the file exists.
     *
     * @return true if the file exists, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean exists() throws IOException;

    /**
     * Creates a new DorisInput for reading the file content in a random-access manner.
     *
     * @return a new DorisInput instance for this file
     * @throws IOException if an I/O error occurs
     */
    DorisInput newInput() throws IOException;

    /**
     * Creates a new DorisInputStream for reading the file content as a stream.
     *
     * @return a new DorisInputStream instance for this file
     * @throws IOException if an I/O error occurs
     */
    DorisInputStream newStream() throws IOException;
}
