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

import java.io.Closeable;
import java.io.IOException;

/**
 * DorisInput provides an abstraction for random-access reading of file content in Doris filesystem layer.
 * <p>
 * This interface allows reading bytes from a specific position in a file into a buffer,
 * supporting both custom and default read operations.
 * It extends Closeable, so implementations must release resources when closed.
 */
public interface DorisInput extends Closeable {

    /**
     * Reads bytes from the file starting at the given position into the specified buffer.
     *
     * @param position the position in the file to start reading from
     * @param buffer the buffer into which bytes are to be transferred
     * @param bufferOffset the offset in the buffer at which bytes will be written
     * @param bufferLength the number of bytes to read
     * @throws IOException if an I/O error occurs
     */
    void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength) throws IOException;

    /**
     * Reads bytes from the file starting at the given position into the entire buffer.
     * This is a convenience default method that delegates to the core readFully method.
     *
     * @param buffer    the buffer into which bytes are to be transferred
     * @param position  the position in the file to start reading from
     * @throws IOException if an I/O error occurs
     */
    default void readFully(byte[] buffer, long position) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }
}
