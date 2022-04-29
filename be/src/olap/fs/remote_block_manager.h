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
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/fs/block_manager.h"
#include "util/file_cache.h"

namespace doris {

class BlockId;
class Env;
class StorageBackend;

namespace fs {

// The remote-backed block manager.
class RemoteBlockManager : public BlockManager {
public:
    // Note: all objects passed as pointers should remain alive for the lifetime
    // of the block manager.
    RemoteBlockManager(Env* local_env, std::shared_ptr<StorageBackend> storage_backend,
                       const BlockManagerOptions& opts);
    virtual ~RemoteBlockManager();

    Status open() override;

    Status create_block(const CreateBlockOptions& opts,
                        std::unique_ptr<WritableBlock>* block) override;
    Status open_block(const FilePathDesc& path_desc,
                      std::unique_ptr<ReadableBlock>* block) override;

    Status get_all_block_ids(std::vector<BlockId>* block_ids) override {
        // TODO(lingbin): to be implemented after we assign each block an id
        return Status::OK();
    };

    Status delete_block(const FilePathDesc& path_desc, bool is_dir = false) override;

    Status link_file(const FilePathDesc& src_path_desc,
                     const FilePathDesc& dest_path_desc) override;

private:
    Env* _local_env;
    std::shared_ptr<StorageBackend> _storage_backend;
    const BlockManagerOptions _opts;
    // Underlying cache instance. Caches opened files.
    std::unique_ptr<FileCache<RandomAccessFile>> _file_cache;
};

} // namespace fs
} // namespace doris