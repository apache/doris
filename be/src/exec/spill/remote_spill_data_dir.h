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

#include <fmt/format.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

#include "exec/spill/spill_data_dir.h"

namespace doris {

/// Spill store on the object storage of a cloud storage vault (spill_storage_type=s3).
///
/// Object layout, relative to the vault prefix:
///   spill/{backend_id}/data/{boot_id}/{query_id}/...   spill data of one boot generation
///   spill/{backend_id}/boots/{boot_id}                  empty marker per boot generation
/// backend_id is FE-assigned and unique per BE (cloud_unique_id is not: every BE added by one
/// ADD BACKEND statement shares it), so no two live processes ever share a prefix. Every object
/// written by this process lives under the current boot_id, so objects under another boot_id
/// always belong to a dead process and can be deleted at startup without racing with running
/// queries; the boots directory lets that cleanup discover generations without listing the data.
///
/// The file system and the object key root are resolved lazily by ensure_ready(): the storage
/// vault and the backend id may not be available when BE starts (both come from meta-service /
/// FE heartbeat).
class RemoteSpillDataDir final : public SpillDataDir {
public:
    /// @param vault_id  storage vault id, empty means the default vault of the instance.
    /// @param boot_id   boot generation of this BE process.
    RemoteSpillDataDir(std::string vault_id, int64_t boot_id);

    Status init() override;

    bool is_remote() const override { return true; }

    bool ready() const override { return _ready.load(std::memory_order_acquire); }

    /// Resolve the storage vault file system and bind it. Idempotent and thread safe. Returns
    /// an error while the backend id or the storage vault is not available yet.
    Status ensure_ready();

    /// Bind a file system directly. Used by ensure_ready() and by tests.
    void init_remote_fs(io::FileSystemSPtr fs, int64_t backend_id);

    /// nullptr until ready.
    io::FileSystemSPtr fs() const override;

    /// Object storage has no capacity to probe; only spill_s3_storage_limit_bytes applies.
    Status update_capacity() override;

    /// Key prefix shared by all boot generations of this BE, spill/{backend_id}.
    const std::string& get_remote_be_root() const { return _remote_be_root; }
    /// spill/{backend_id}/data/{boot_id} — the data of one boot generation.
    std::string get_remote_boot_data_path(std::string_view boot_id) const {
        return fmt::format("{}/data/{}", _remote_be_root, boot_id);
    }
    /// spill/{backend_id}/boots/{boot_id} — an empty marker object written once the store is
    /// ready and refreshed daily by the GC thread. The meta-service recycler deletes markers
    /// like any other spill object once they are older than spill_objects_expire_time_second
    /// (> 1 day by contract); the daily refresh keeps the marker of a live process alive, and a
    /// generation whose marker is gone (crash before the first GC round, TTL misconfigured) is
    /// only reclaimed by that recycler.
    std::string get_remote_boot_marker_path(std::string_view boot_id) const {
        return fmt::format("{}/boots/{}", _remote_be_root, boot_id);
    }
    std::string get_remote_boots_path() const { return fmt::format("{}/boots", _remote_be_root); }

    int64_t backend_id() const { return _backend_id; }
    int64_t boot_id() const { return _boot_id; }
    const std::string& vault_id() const { return _vault_id; }

protected:
    bool _reach_limit_unlocked(int64_t incoming_data_size) override;

private:
    std::string _vault_id;
    int64_t _boot_id = 0;
    int64_t _backend_id = 0;
    std::mutex _init_mutex;
    std::atomic<bool> _ready {false};
    io::FileSystemSPtr _fs;
    std::string _remote_be_root;
};

} // namespace doris
