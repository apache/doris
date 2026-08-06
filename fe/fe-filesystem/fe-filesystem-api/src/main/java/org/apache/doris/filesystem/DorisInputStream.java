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

import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} that supports random-access seeking.
 *
 * <p>Required by Iceberg's {@code SeekableInputStream} and other consumers that
 * need to re-read data at arbitrary offsets within a file.
 */
public abstract class DorisInputStream extends InputStream {

    /**
     * Returns the current byte position in the stream (0-based offset from start of file).
     *
     * @throws IOException if the position cannot be determined
     */
    public abstract long getPos() throws IOException;

    /**
     * Seeks to the specified byte position.
     * The next {@link #read()} will return the byte at {@code pos}.
     *
     * @param pos the byte offset to seek to (must be &ge; 0 and &le; file length)
     * @throws IOException if the seek fails or {@code pos} is out of range
     */
    public abstract void seek(long pos) throws IOException;
}
