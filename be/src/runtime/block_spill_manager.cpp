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

namespace doris {
BlockSpillManager::BlockSpillManager(const std::vector<StorePath>& paths) : _store_paths(paths) {}

Status BlockSpillManager::get_writer(int32_t batch_size, vectorized::BlockSpillWriterUPtr& writer) {
    int64_t id;
    std::vector<int> indices(_store_paths.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937 {std::random_device {}()});

    std::string path = _store_paths[indices[0]].path;
    std::string unique_name = boost::uuids::to_string(boost::uuids::random_generator()());
    path += "/" + unique_name;
    {
        std::lock_guard<std::mutex> l(lock_);
        id = id_++;
        id_to_file_paths_[id] = path;
    }

    writer.reset(new vectorized::BlockSpillWriter(id, batch_size, path));
    return writer->open();
}

Status BlockSpillManager::get_reader(int64_t stream_id, vectorized::BlockSpillReaderUPtr& reader) {
    std::string path;
    {
        std::lock_guard<std::mutex> l(lock_);
        DCHECK(id_to_file_paths_.end() != id_to_file_paths_.find(stream_id));
        path = id_to_file_paths_[stream_id];
    }
    reader.reset(new vectorized::BlockSpillReader(path));
    return reader->open();
}

void BlockSpillManager::remove(int64_t stream_id) {
    std::lock_guard<std::mutex> l(lock_);
    id_to_file_paths_.erase(stream_id);
}
} // namespace doris