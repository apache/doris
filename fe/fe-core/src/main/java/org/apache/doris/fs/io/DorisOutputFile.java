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
import java.io.OutputStream;

/**
 * DorisOutputFile represents an abstraction for a file output destination in the Doris filesystem layer.
 * <p>
 * This interface provides methods to create output streams for writing file content,
 * either as a new file or by overwriting an existing file.
 * It also provides a method to retrieve the file's path.
 * Implementations of this interface allow Doris to interact with various file systems
 * in a unified way for output operations.
 */
public interface DorisOutputFile {
    /**
     * Creates a new file and returns an OutputStream for writing to it.
     * <p>
     * If the file already exists, this method may fail depending on the implementation.
     *
     * @return an OutputStream for writing to the new file
     * @throws IOException if an I/O error occurs or the file already exists
     */
    OutputStream create() throws IOException;

    /**
     * Creates a new file or overwrites the existing file and returns an OutputStream for writing to it.
     * <p>
     * If the file already exists, it will be overwritten.
     *
     * @return an OutputStream for writing to the file
     * @throws IOException if an I/O error occurs
     */
    OutputStream createOrOverwrite() throws IOException;

    /**
     * Returns the path of this output file as a ParsedPath object.
     *
     * @return the ParsedPath representing the file location
     */
    ParsedPath path();
}
