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

#include <atomic>
#include <mutex>
#include <string>

#include "exec/spill/spill_data_dir.h"

namespace doris {

/// Spill store on the object storage of a cloud storage vault (spill_storage_type=s3).
///
/// Object layout, relative to the vault prefix:
///   spill/{ip}_{port}/{query_id}/{spill file}/{part}
/// {ip}_{port} is the address this BE advertises (BackendOptions::get_localhost(), normally its
/// IP) and its heartbeat_service_port, the pair FE identifies a BE by, so several BEs on one
/// host get different directories and a key tells which BE wrote it and which query it belongs
/// to. The objects of a spill file are deleted when its SpillFile is destroyed, which happens
/// when the query is done with it; a deletion that failed is retried by the GC thread. Nothing
/// else deletes spill objects: what a BE that crashed left behind stays until an object
/// lifecycle rule of the bucket (expiring keys under spill/ and aborting incomplete multipart
/// uploads) removes it.
///
/// The file system is resolved lazily by ensure_ready(): the storage vault may not be known
/// yet when BE starts.
class RemoteSpillDataDir final : public SpillDataDir {
public:
    /// @param vault_id  storage vault id, empty means the default vault of the instance.
    explicit RemoteSpillDataDir(std::string vault_id);

    Status init() override;

    bool is_remote() const override { return true; }

    bool ready() const override { return _ready.load(std::memory_order_acquire); }

    /// Resolve the storage vault file system and bind it. Idempotent and thread safe. Returns
    /// an error while the storage vault is not available yet.
    Status ensure_ready();

    /// Bind a file system directly. Used by ensure_ready() and by tests.
    void init_remote_fs(io::FileSystemSPtr fs, std::string endpoint);

    /// nullptr until ready.
    io::FileSystemSPtr fs() const override;

    /// Object storage has no capacity to probe; only spill_s3_storage_limit_bytes applies.
    Status update_capacity() override;

    /// "{ip}_{port}" in the object keys; empty until ready.
    const std::string& endpoint() const { return _endpoint; }

    const std::string& vault_id() const { return _vault_id; }

protected:
    bool _reach_limit_unlocked(int64_t incoming_data_size) override;

private:
    std::string _vault_id;
    std::string _endpoint;
    std::mutex _init_mutex;
    std::atomic<bool> _ready {false};
    io::FileSystemSPtr _fs;
};

} // namespace doris
