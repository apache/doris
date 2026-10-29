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

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "common/status.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// One immutable generation of an ObjClientHolder. The config and the client built from it
// are published together, so a caller never pairs a new config with an old client. For
// example, a reset from ak1 to ak2 that races with display_path() and get() lets each
// caller see either (ak1 config, ak1 client) or (ak2 config, ak2 client), never a mix.
struct ObjClientState {
    ObjClientConfig config;
    // Null until init() succeeds.
    std::shared_ptr<ObjStorageClient> client;
    // Increases by one on every successful publish (init or reset).
    uint64_t generation = 0;
};

// In runtime, credentials may be modified, and the original client instance will be replaced.
// The ObjStorageFileReader cached by the Segment must hold a shared ObjClientHolder in order
// to access the data with the latest credentials.
//
// The holder owns an immutable ObjClientState and replaces it as a whole. A request takes
// a snapshot and calls the SDK outside the lock; requests that took the old snapshot keep
// using the old client until they finish, later requests see the new one.
class ObjClientHolder {
public:
    explicit ObjClientHolder(ObjClientConfig config);
    ~ObjClientHolder();

    Status init();

    // Merges a refreshed configuration (merge_obj_client_config) and replaces
    // the client if the result differs. This method is threadsafe. Resource and Storage Vault
    // refreshes are the callers.
    //
    // The new client is built outside the lock and published only if no other reset or
    // init published in between. Otherwise the update is rejected rather than applied on
    // top of a state it was not merged with, so an older config cannot overwrite a newer
    // one; the caller keeps the current state and retries with its next refresh.
    Status reset(const ObjClientConfig& update);

    std::shared_ptr<const ObjClientState> state() const {
        std::shared_lock lock(_mtx);
        return _state;
    }

    // Called for every remote read, so copy only the client rather than the whole snapshot.
    std::shared_ptr<ObjStorageClient> get() const {
        std::shared_lock lock(_mtx);
        return _state->client;
    }

    Result<int64_t> object_file_size(const std::string& bucket, const std::string& key) const;

    // For error messages: endpoint/bucket/key.
    std::string display_path(std::string_view bucket, std::string_view key) const;

#ifdef BE_TEST
    // Publishes a new generation that keeps the current config and uses `client`.
    void set_client_for_test(std::shared_ptr<ObjStorageClient> client) {
        std::lock_guard lock(_mtx);
        _state = std::make_shared<ObjClientState>(ObjClientState {
                .config = _state->config,
                .client = std::move(client),
                .generation = _state->generation + 1,
        });
    }
#endif

private:
    // Replaces the state with (config, client) if `expected` is still current.
    Status _publish(const std::shared_ptr<const ObjClientState>& expected, ObjClientConfig config,
                    std::shared_ptr<ObjStorageClient> client);

    mutable std::shared_mutex _mtx;
    std::shared_ptr<const ObjClientState> _state;
};

} // namespace doris::io
