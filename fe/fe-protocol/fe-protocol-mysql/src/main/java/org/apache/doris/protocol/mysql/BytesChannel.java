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

package org.apache.doris.protocol.mysql;

import java.nio.ByteBuffer;

/**
 * Interface for byte channel operations.
 * 
 * <p>This interface abstracts the byte reading operations needed by
 * the MySQL protocol implementation, allowing for different channel
 * implementations (socket-based, proxy, etc.).
 * 
 * @since 2.0.0
 */
public interface BytesChannel {
    
    /**
     * Reads bytes into the destination buffer.
     * 
     * <p>This method will block until data is available or the channel is closed.
     * 
     * @param dstBuf destination buffer
     * @return number of bytes read, or -1 if channel is closed
     */
    int read(ByteBuffer dstBuf);

    /**
     * Tests if data can be read within a timeout.
     * 
     * <p>This method attempts to read into the destination buffer with a timeout.
     * It's useful for checking if the client has sent data without blocking indefinitely.
     * 
     * @param dstBuf destination buffer (should have exactly 1 byte remaining)
     * @param timeoutMs timeout in milliseconds
     * @return number of bytes read, 0 if timeout, or -1 if channel is closed
     */
    int testReadWithTimeout(ByteBuffer dstBuf, long timeoutMs);
}
