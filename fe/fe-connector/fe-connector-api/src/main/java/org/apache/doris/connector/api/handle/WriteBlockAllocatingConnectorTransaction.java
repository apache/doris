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

package org.apache.doris.connector.api.handle;

/**
 * Narrow opt-in capability for a {@link ConnectorTransaction} with a stateful write session that hands
 * out block ids through a write-time BE&rarr;FE callback (only maxcompute today).
 *
 * <p>Kept OFF {@link ConnectorTransaction} so the shared transaction contract carries no source-specific
 * methods: the engine's write-block RPC path checks {@code instanceof} before it calls, turning
 * "unsupported" from a runtime throw into a type mismatch. A connector without a block-allocating write
 * session simply does not implement this.</p>
 */
public interface WriteBlockAllocatingConnectorTransaction {

    /**
     * Allocates a contiguous range of write block ids for the given write session, returning the first
     * allocated id. Called from the BE&rarr;FE RPC path during a write.
     *
     * @param writeSessionId opaque connector-defined write session identifier
     * @param count          number of block ids to allocate
     * @return the first allocated block id
     */
    long allocateWriteBlockRange(String writeSessionId, long count);
}
