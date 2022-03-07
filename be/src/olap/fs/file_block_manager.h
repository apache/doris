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
class MemTracker;
class RandomAccessFile;

namespace fs {
namespace internal {

class FileReadableBlock;
class FileWritableBlock;
struct BlockManagerMetrics;

} // namespace internal

// TODO(lingbin): When we create a batch of blocks(blocks are created one by one),
// eg, when we do a compaction,  multiple files will be generated in sequence.
// For this scenario, we should have a mechanism that can give the Operating System
// more opportunities to perform IO merge.

// A file-backed block storage implementation.
//
// This is a naive block implementation which maps each block to its own
// file on disk.
//
// The block manager can take advantage of multiple filesystem paths.
//
// When creating blocks, the block manager will place blocks based on the
// provided CreateBlockOptions.

// The file-backed block manager.
class FileBlockManager : public BlockManager {
public:
    // Note: all objects passed as pointers should remain alive for the lifetime
    // of the block manager.
    FileBlockManager(Env* env, BlockManagerOptions opts);
    virtual ~FileBlockManager();

    Status open() override;

    Status create_block(const CreateBlockOptions& opts,
                        std::unique_ptr<WritableBlock>* block) override;
    Status open_block(const FilePathDesc& path_desc,
                      std::unique_ptr<ReadableBlock>* block) override;

    Status get_all_block_ids(std::vector<BlockId>* block_ids) override {
        // TODO(lingbin): to be implemented after we assign each block an id
        return Status::OK();
    };

    // Deletes an existing block, allowing its space to be reclaimed by the
    // filesystem. The change is immediately made durable.
    //
    // Blocks may be deleted while they are open for reading or writing;
    // the actual deletion will take place after the last open reader or
    // writer is closed.
    // is_dir: whether this path is a dir or file. if it is true, delete all files in this path
    Status delete_block(const FilePathDesc& path_desc, bool is_dir = false) override;

    Status link_file(const FilePathDesc& src_path_desc,
                     const FilePathDesc& dest_path_desc) override;

private:
    friend class internal::FileReadableBlock;
    friend class internal::FileWritableBlock;

    // Synchronizes the metadata for a block with the given location.
    Status _sync_metadata(const FilePathDesc& path_desc);

    Env* env() const { return _env; }

    // For manipulating files.
    Env* _env;

    // The options that the FileBlockManager was created with.
    const BlockManagerOptions _opts;

    // Tracks the block directories which are dirty from block creation. This
    // lets us perform some simple coalescing when synchronizing metadata.
    std::unordered_set<std::string> _dirty_dirs;

    // Metric container for the block manager.
    // May be null if instantiated without metrics.
    std::unique_ptr<internal::BlockManagerMetrics> _metrics;

    // Tracks memory consumption of any allocations numerous enough to be
    // interesting.
    std::shared_ptr<MemTracker> _mem_tracker;

    // DISALLOW_COPY_AND_ASSIGN(FileBlockManager);

    // Underlying cache instance. Caches opened files.
    std::unique_ptr<FileCache<RandomAccessFile>> _file_cache;
};

} // namespace fs
} // namespace doris
