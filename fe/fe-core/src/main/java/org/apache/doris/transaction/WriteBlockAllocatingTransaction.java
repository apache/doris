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

package org.apache.doris.transaction;

import org.apache.doris.common.UserException;

/**
 * Narrow opt-in capability for a {@link Transaction} whose write allocates block ranges through a
 * write-time BE&rarr;FE callback (only maxcompute today).
 *
 * <p>Kept OFF the shared {@link Transaction} contract so the generic engine transaction carries no
 * source-specific methods: the write-block RPC handler ({@code FrontendServiceImpl.getMaxComputeBlockIdRange})
 * checks {@code instanceof WriteBlockAllocatingTransaction} before it calls, turning "unsupported" from a
 * runtime throw into a type mismatch.</p>
 */
public interface WriteBlockAllocatingTransaction extends Transaction {

    /**
     * Allocates a contiguous range of write block ids for the given write session, returning the first
     * allocated id.
     *
     * @param writeSessionId opaque connector-defined write session identifier
     * @param count          number of block ids to allocate
     * @return the first allocated block id
     * @throws UserException on validation failure or allocation overflow
     */
    long allocateWriteBlockRange(String writeSessionId, long count) throws UserException;
}
