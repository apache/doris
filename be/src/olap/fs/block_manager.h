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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "env/env.h"

namespace doris {

class BlockId;
class Env;
class MemTracker;
struct Slice;

namespace fs {

class BlockManager;

// The smallest unit of data that is backed by the filesystem.
//
// The block interface reflects Doris on-disk storage design principles:
// - Blocks are append only.
// - Blocks are immutable once written.
// - Blocks opened for reading are thread-safe and may be used by multiple
//   concurrent readers.
// - Blocks opened for writing are not thread-safe.
class Block {
public:
    virtual ~Block() {}

    // Returns the identifier for this block.
    // TODO: should we assign a block an identifier?
    virtual const BlockId& id() const = 0;

    // Currently, each block in Doris will correspond to a file, but it may not be
    // in the future (that is, a block may correspond to multiple files, or multiple
    // blocks correspond to a file).
    // For convenience, the path interface is directly exposed. At that time, the path()
    // method should be removed.
    virtual const FilePathDesc& path_desc() const = 0;
};

// A block that has been opened for writing. There may only be a single
// writing thread, and data may only be appended to the block.
//
// close() is an expensive operation, as it must flush both dirty block data
// and metadata to disk. The block manager API provides two ways to improve
// close() performance:
// 1. finalize() before close(). When 'block_manager_preflush_control' is set
//    to 'finalize', if there's enough work to be done between the two calls,
//    there will be less outstanding I/O to wait for during close().
// 2. CloseBlocks() on a group of blocks. This ensures: 1) flushing of dirty
//    blocks are grouped together if possible, resulting in less I/O.
//    2) when waiting on outstanding I/O, the waiting is done in parallel.
//
// NOTE: if a WritableBlock is not explicitly close()ed, it will be aborted
// (i.e. deleted).
class WritableBlock : public Block {
public:
    enum State {
        // There is no dirty data in the block.
        CLEAN,
        // There is some dirty data in the block.
        DIRTY,
        // No more data may be written to the block, but it is not yet guaranteed
        // to be durably stored on disk.
        FINALIZED,
        // The block is closed. No more operations can be performed on it.
        CLOSED
    };

    // Destroy the WritableBlock. If it was not explicitly closed using close(),
    // this will Abort() the block.
    virtual ~WritableBlock() {}

    // Destroys the in-memory representation of the block and synchronizes
    // dirty block data and metadata with the disk. On success, guarantees
    // that the entire block is durable.
    virtual Status close() = 0;

    // Like close() but does not synchronize dirty data or metadata to disk.
    // Meaning, after a successful Abort(), the block no longer exists.
    virtual Status abort() = 0;

    // Get a pointer back to this block's manager.
    virtual BlockManager* block_manager() const = 0;

    // Appends the chunk of data referenced by 'data' to the block.
    //
    // Does not guarantee durability of 'data'; close() must be called for all
    // outstanding data to reach the disk.
    virtual Status append(const Slice& data) = 0;

    // Appends multiple chunks of data referenced by 'data' to the block.
    //
    // Does not guarantee durability of 'data'; close() must be called for all
    // outstanding data to reach the disk.
    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    // Signals that the block will no longer receive writes. Does not guarantee
    // durability; close() must still be called for that.
    //
    // When 'block_manager_preflush_control' is set to 'finalize', it also begins an
    // asynchronous flush of dirty block data to disk. If there is other work
    // to be done between the final Append() and the future close(),
    // finalize() will reduce the amount of time spent waiting for outstanding
    // I/O to complete in close(). This is analogous to readahead or prefetching.
    virtual Status finalize() = 0;

    // Returns the number of bytes successfully appended via Append().
    virtual size_t bytes_appended() const = 0;

    virtual State state() const = 0;
};

// A block that has been opened for reading. Multiple in-memory blocks may
// be constructed for the same logical block, and the same in-memory block
// may be shared amongst threads for concurrent reading.
class ReadableBlock : public Block {
public:
    virtual ~ReadableBlock() {}

    // Destroys the in-memory representation of the block.
    virtual Status close() = 0;

    // Get a pointer back to this block's manager.
    virtual BlockManager* block_manager() const = 0;

    // Returns the on-disk size of a written block.
    virtual Status size(uint64_t* sz) const = 0;

    // Reads exactly 'result.size' bytes beginning from 'offset' in the block,
    // returning an error if fewer bytes exist.
    // Sets "result" to the data that was read.
    // If an error was encountered, returns a non-OK status.
    virtual Status read(uint64_t offset, Slice result) const = 0;

    // Reads exactly the "results" aggregate bytes, based on each Slice's "size",
    // beginning from 'offset' in the block, returning an error if fewer bytes exist.
    // Sets each "result" to the data that was read.
    // If an error was encountered, returns a non-OK status.
    virtual Status readv(uint64_t offset, const Slice* res, size_t res_cnt) const = 0;

    // Returns the memory usage of this object including the object itself.
    // virtual size_t memory_footprint() const = 0;
};

// Provides options and hints for block placement. This is used for identifying
// the correct DataDirGroups to place blocks. In the future this may also be
// used to specify directories based on block type (e.g. to prefer bloom block
// placement into SSD-backed directories).
struct CreateBlockOptions {
    CreateBlockOptions(const FilePathDesc& new_path_desc) { path_desc = new_path_desc; }
    CreateBlockOptions(const std::string& path) { path_desc.filepath = path; }
    // const std::string tablet_id;
    FilePathDesc path_desc;
};

// Block manager creation options.
struct BlockManagerOptions {
    BlockManagerOptions() = default;

    // The memory tracker under which all new memory trackers will be parented.
    // If nullptr, new memory trackers will be parented to the root tracker.
    std::shared_ptr<MemTracker> parent_mem_tracker;

    // If false, metrics will not be produced.
    bool enable_metric = false;

    // Whether the block manager should only allow reading. Defaults to false.
    bool read_only = false;
};

// Utilities for Block lifecycle management. All methods are thread-safe.
class BlockManager {
public:
    // Lists the available block manager types.
    static std::vector<std::string> block_manager_types() { return {"file"}; }

    virtual ~BlockManager() {}

    // Opens an existing on-disk representation of this block manager and
    // checks it for inconsistencies. If found, and if the block manager was not
    // constructed in read-only mode, an attempt will be made to repair them.
    //
    // If 'report' is not nullptr, it will be populated with the results of the
    // check (and repair, if applicable); otherwise, the results of the check
    // will be logged and the presence of fatal inconsistencies will manifest as
    // a returned error.
    //
    // Returns an error if an on-disk representation does not exist or cannot be
    // opened.
    virtual Status open() = 0;

    // Creates a new block using the provided options and opens it for
    // writing. The block's ID will be generated.
    //
    // Does not guarantee the durability of the block; it must be closed to
    // ensure that it reaches disk.
    //
    // Does not modify 'block' on error.
    virtual Status create_block(const CreateBlockOptions& opts,
                                std::unique_ptr<WritableBlock>* block) = 0;

    // Opens an existing block for reading.
    //
    // While it is safe to delete a block that has already been opened, it is
    // not safe to do so concurrently with the OpenBlock() call itself. In some
    // block manager implementations this may result in unusual behavior. For
    // example, OpenBlock() may succeed but subsequent ReadableBlock operations
    // may fail.
    //
    // Does not modify 'block' on error.
    virtual Status open_block(const FilePathDesc& path_desc,
                              std::unique_ptr<ReadableBlock>* block) = 0;

    // Retrieves the IDs of all blocks under management by this block manager.
    // These include ReadableBlocks as well as WritableBlocks.
    //
    // Returned block IDs are not guaranteed to be in any particular order,
    // nor is the order guaranteed to be deterministic. Furthermore, if
    // concurrent operations are ongoing, some of the blocks themselves may not
    // even exist after the call.
    virtual Status get_all_block_ids(std::vector<BlockId>* block_ids) = 0;

    virtual Status delete_block(const FilePathDesc& path_desc, bool is_dir = false) = 0;

    virtual Status link_file(const FilePathDesc& src_path_desc,
                             const FilePathDesc& dest_path_desc) = 0;

    static const std::string block_manager_preflush_control;
};

} // namespace fs
} // namespace doris
