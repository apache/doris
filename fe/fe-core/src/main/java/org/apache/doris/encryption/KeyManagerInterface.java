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

package org.apache.doris.encryption;

import org.apache.doris.persist.KeyOperationInfo;

import java.util.List;

/**
 * Interface for managing cryptographic keys, including root key setup,
 * initialization, editlog replay, and retrieval of master keys.
 */
public interface KeyManagerInterface {

    /**
     * Sets the root key and records its basic metadata.
     *
     * @param info
     *        information about the root key (e.g. key identifier, creation timestamp,
     *        algorithm, usage constraints)
     * @throws RuntimeException
     *         if the provided root key info is invalid, corrupt, or violates security policies
     */
    public void setRootKey(RootKeyInfo info) throws RuntimeException;

    /**
     * Performs initialization logic for key management.
     * Typical use cases:
     *  - Load persisted keys from secure storage,
     *  - Validate root key and master keys,
     *  - Set up internal data structures or caches.
     * Should be called before any key operations or queries.
     */
    public void init();

    /**
     * Replays a key operation event, such as key creation, rotation, or deletion.
     * This is typically used during system recovery, startup, or replica synchronization
     * to ensure key states remain consistent across nodes.
     *
     * @param keyOpInfo The key operation information to be replayed.
     */
    public void replayKeyOperation(KeyOperationInfo keyOpInfo);

    /**
     * Retrieves all registered master (or derived) encryption keys.
     *
     * @return a list of EncryptionKey objects representing the current master keys
     *         (may include versioning or metadata as part of each key object)
     */
    public List<EncryptionKey> getAllMasterKeys();
}
