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

#include "runtime/block_spill_manager.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <random>

#include "env/env_posix.h"
#include "util/file_utils.h"
#include "util/time.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"

namespace doris {
static const std::string BLOCK_SPILL_DIR = "spill";
static const std::string BLOCK_SPILL_GC_DIR = "spill_gc";
BlockSpillManager::BlockSpillManager(const std::vector<StorePath>& paths) : _store_paths(paths) {}

Status BlockSpillManager::init() {
    for (const auto& path : _store_paths) {
        auto dir = fmt::format("{}/{}", path.path, BLOCK_SPILL_GC_DIR);
        if (!FileUtils::check_exist(dir)) {
            RETURN_IF_ERROR(FileUtils::create_dir(dir));
        }

        dir = fmt::format("{}/{}", path.path, BLOCK_SPILL_DIR);
        if (!FileUtils::check_exist(dir)) {
            RETURN_IF_ERROR(FileUtils::create_dir(dir));
        } else {
            auto suffix = ToStringFromUnixMillis(UnixMillis());
            auto gc_dir = fmt::format("{}/{}/{}", path.path, BLOCK_SPILL_GC_DIR, suffix);
            if (Env::Default()->rename_dir(dir, gc_dir).ok()) {
                RETURN_IF_ERROR(FileUtils::create_dir(dir));
            }
        }
    }

    return Status::OK();
}

void BlockSpillManager::gc(int64_t max_file_count) {
    if (max_file_count < 1) {
        return;
    }
    int64_t count = 0;
    for (const auto& path : _store_paths) {
        std::string gc_root_dir = fmt::format("{}/{}", path.path, BLOCK_SPILL_GC_DIR);

        std::set<std::string> dirs;
        auto st = FileUtils::list_dirs_files(gc_root_dir, &dirs, nullptr, Env::Default());
        if (!st.ok()) {
            continue;
        }
        for (const auto& dir : dirs) {
            std::string abs_dir = fmt::format("{}/{}", gc_root_dir, dir);

            std::set<std::string> files;
            st = FileUtils::list_dirs_files(abs_dir, nullptr, &files, Env::Default());
            if (!st.ok()) {
                continue;
            }
            if (files.empty()) {
                FileUtils::remove(abs_dir);
                if (count++ == max_file_count) {
                    return;
                }
                continue;
            }
            for (const auto& file : files) {
                auto abs_file_path = fmt::format("{}/{}", abs_dir, file);
                FileUtils::remove(abs_file_path);
                if (count++ == max_file_count) {
                    return;
                }
            }
        }
    }
}

Status BlockSpillManager::get_writer(int32_t batch_size, vectorized::BlockSpillWriterUPtr& writer,
                                     RuntimeProfile* profile) {
    int64_t id;
    std::vector<int> indices(_store_paths.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937 {std::random_device {}()});

    std::string path = _store_paths[indices[0]].path + "/" + BLOCK_SPILL_DIR;
    std::string unique_name = boost::uuids::to_string(boost::uuids::random_generator()());
    path += "/" + unique_name;
    {
        std::lock_guard<std::mutex> l(lock_);
        id = id_++;
        id_to_file_paths_[id] = path;
    }

    writer.reset(new vectorized::BlockSpillWriter(id, batch_size, path, profile));
    return writer->open();
}

Status BlockSpillManager::get_reader(int64_t stream_id, vectorized::BlockSpillReaderUPtr& reader,
                                     RuntimeProfile* profile, bool delete_after_read) {
    std::string path;
    {
        std::lock_guard<std::mutex> l(lock_);
        DCHECK(id_to_file_paths_.end() != id_to_file_paths_.find(stream_id));
        path = id_to_file_paths_[stream_id];
    }
    reader.reset(new vectorized::BlockSpillReader(stream_id, path, profile, delete_after_read));
    return reader->open();
}

void BlockSpillManager::remove(int64_t stream_id) {
    std::lock_guard<std::mutex> l(lock_);
    id_to_file_paths_.erase(stream_id);
}
} // namespace doris