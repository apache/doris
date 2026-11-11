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

#include "exec/spill/spill_data_dir.h"

namespace doris {

/// Spill store on one local disk (spill_storage_type=local).
class LocalSpillDataDir final : public SpillDataDir {
public:
    LocalSpillDataDir(std::string path, int64_t capacity_bytes,
                      TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init() override;

    io::FileSystemSPtr fs() const override;

    Status update_capacity() override;

    double get_disk_usage(int64_t incoming_data_size) const override {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    /// {path}/spill_gc[/sub_dir]: spill directories moved here are deleted by the GC thread.
    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

protected:
    bool _reach_limit_unlocked(int64_t incoming_data_size,
                               const std::lock_guard<std::mutex>& lock) override;

private:
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
    void _update_inode_usage();
};

} // namespace doris
