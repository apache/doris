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

#include "vec/spill/spill_stream_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <numeric>
#include <random>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/spill/spill_stream.h"

namespace doris {

namespace vectorized {

static const std::string SPILL_DIR = "spill";
static const std::string SPILL_GC_DIR = "spill_gc";

SpillStreamManager::SpillStreamManager(const std::vector<StorePath>& paths) : _store_paths(paths) {}

Status SpillStreamManager::init() {
    for (const auto& path : _store_paths) {
        auto dir = fmt::format("{}/{}", path.path, SPILL_GC_DIR);
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(dir));
        }

        dir = fmt::format("{}/{}", path.path, SPILL_DIR);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(dir));
        } else {
            auto suffix = ToStringFromUnixMillis(UnixMillis());
            auto gc_dir = fmt::format("{}/{}/{}", path.path, SPILL_GC_DIR, suffix);
            RETURN_IF_ERROR(io::global_local_filesystem()->rename_dir(dir, gc_dir));
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(dir));
        }
    }
    return Status::OK();
}

Status SpillStreamManager::register_spill_stream(SpillStreamSPtr& spill_stream,
                                                 std::string query_id, std::string operator_name,
                                                 int32_t operator_id, /* int32_t task_id,*/
                                                 int32_t batch_size, RuntimeProfile* profile) {
    int64_t id;
    std::vector<int> indices(_store_paths.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937 {std::random_device {}()});

    std::string spill_root_dir = fmt::format("{}/{}", _store_paths[indices[0]].path, SPILL_DIR);
    std::string spill_dir = fmt::format("{}/{}/{}-{}-{}", spill_root_dir, query_id, operator_name,
                                        operator_id, /*task_id*/ 0);
    {
        std::lock_guard<std::mutex> l(lock_);
        id = id_++;
        // id_to_file_paths_[id] = path;
    }
    spill_stream = std::make_shared<SpillStream>(id, spill_dir, batch_size, profile);
    RETURN_IF_ERROR(spill_stream->prepare());
    {
        std::lock_guard<std::mutex> l(lock_);
        id_to_spill_streams_[id] = spill_stream;
    }
    return Status::OK();
}

void SpillStreamManager::delete_spill_stream(SpillStreamSPtr spill_stream) {
    std::lock_guard<std::mutex> l(lock_);
    id_to_spill_streams_.erase(spill_stream->id());
}
} // namespace vectorized
} // namespace doris
