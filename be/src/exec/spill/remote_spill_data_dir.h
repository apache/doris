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
namespace io {
class RemoteFileSystem;
} // namespace io

/// Spill store on the object storage of a cloud storage vault (spill_storage_type=remote).
///
/// Object layout, relative to the vault prefix:
///   spill/{backend_uid}/{query_id}/{spill file}/{part}
/// A random process-scoped ID avoids exposing this BE's address in object keys. A restart
/// gets a new root; old remote objects remain subject to the bucket lifecycle rule.
/// SpillFile destruction queues its objects for the GC thread once the query is done
/// with them; failed deletions remain queued for retry. Nothing
/// else deletes spill objects: what a BE that crashed left behind stays until an object
/// lifecycle rule of the bucket (expiring keys under <vault prefix>/spill/ and aborting
/// incomplete multipart uploads) removes it.
///
/// The file system is resolved lazily by ensure_ready(): the storage vault may not be known
/// yet when BE starts. See select_vault_fs() for which vault is used. Every SpillFile keeps the
/// file system it was created on, so its objects are read and deleted on that vault even when
/// a later file is created on another one.
class RemoteSpillDataDir final : public SpillDataDir {
public:
    /// @param vault_id  storage vault ID, empty selects one automatically (see select_vault_fs()).
    explicit RemoteSpillDataDir(std::string vault_id);

    /// The file system spill writes new files to:
    ///   1. `vault_id` (spill_s3_storage_vault_id) when it is set;
    ///   2. otherwise `latest_fs`, which only an instance without storage vaults has;
    ///   3. otherwise the S3 storage vault with the largest ID (the newest one), so that
    ///      changing the default vault does not move spill.
    /// nullptr when no such vault is known.
    static std::shared_ptr<io::RemoteFileSystem> select_vault_fs(
            const std::string& vault_id, const std::shared_ptr<io::RemoteFileSystem>& latest_fs);

    Status init() override;

    bool is_remote() const override { return true; }

    bool ready() const override { return _ready.load(std::memory_order_acquire); }

    /// Resolve the vault for a new file and return an immutable filesystem snapshot. Without
    /// spill_s3_storage_vault_id every call selects again; existing files retain their FS.
    Status ensure_ready(io::FileSystemSPtr* file_fs);

    /// nullptr until ready.
    io::FileSystemSPtr fs() const override;

    /// Object storage has no capacity to probe; only spill_storage_limit_bytes applies.
    Status update_capacity() override;

    void record_persisted_bytes(int64_t bytes) override;
    void release_persisted_bytes(int64_t bytes) override;
    int64_t get_persisted_bytes() override;

protected:
    bool _reach_limit_unlocked(int64_t incoming_data_size,
                               const std::lock_guard<std::mutex>& lock) override;

private:
    void _bind_fs_unlocked(io::FileSystemSPtr fs);
    std::string _vault_id;
    mutable std::mutex _init_mutex;
    std::atomic<bool> _ready {false};
    io::FileSystemSPtr _fs;
    int64_t _persisted_bytes = 0;
};

} // namespace doris
