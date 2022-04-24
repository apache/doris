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

#include "olap/fs/remote_block_manager.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "env/env_posix.h"
#include "env/env_util.h"
#include "gutil/strings/substitute.h"
#include "olap/fs/block_id.h"
#include "olap/fs/cached_segment_loader.h"
#include "olap/storage_engine.h"
#include "util/filesystem_util.h"
#include "util/storage_backend.h"
#include "util/storage_backend_mgr.h"

using std::shared_ptr;
using std::string;

using strings::Substitute;

namespace doris {
namespace fs {

namespace internal {

////////////////////////////////////////////////////////////
// RemoteWritableBlock
////////////////////////////////////////////////////////////

// A remote-backed block that has been opened for writing.
//
// Contains a pointer to the block manager as well as file path
// so that dirty metadata can be synced via BlockManager::SyncMetadata()
// at Close() time. Embedding a file path (and not a simpler
// BlockId) consumes more memory, but the number of outstanding
// RemoteWritableBlock instances is expected to be low.
class RemoteWritableBlock : public WritableBlock {
public:
    RemoteWritableBlock(RemoteBlockManager* block_manager, const FilePathDesc& path_desc,
                        shared_ptr<WritableFile> writer);

    virtual ~RemoteWritableBlock();

    virtual Status close() override;

    virtual Status abort() override;

    virtual BlockManager* block_manager() const override;

    virtual const BlockId& id() const override;
    virtual const FilePathDesc& path_desc() const override;

    virtual Status append(const Slice& data) override;

    virtual Status appendv(const Slice* data, size_t data_cnt) override;

    virtual Status finalize() override;

    virtual size_t bytes_appended() const override;

    virtual State state() const override;

    void handle_error(const Status& s) const;

    // Starts an asynchronous flush of dirty block data to disk.
    Status flush_data_async();

private:
    DISALLOW_COPY_AND_ASSIGN(RemoteWritableBlock);

    enum SyncMode { SYNC, NO_SYNC };

    // Close the block, optionally synchronizing dirty data and metadata.
    Status _close(SyncMode mode);

    // Back pointer to the block manager.
    //
    // Should remain alive for the lifetime of this block.
    RemoteBlockManager* _block_manager;

    const BlockId _block_id;
    FilePathDesc _path_desc;

    // The underlying opened file backing this block.
    shared_ptr<WritableFile> _local_writer;

    State _state;

    // The number of bytes successfully appended to the block.
    size_t _bytes_appended;
};

RemoteWritableBlock::RemoteWritableBlock(RemoteBlockManager* block_manager, const FilePathDesc& path_desc,
                                         shared_ptr<WritableFile> local_writer)
        : _block_manager(block_manager),
          _path_desc(path_desc),
          _local_writer(std::move(local_writer)) {
}

RemoteWritableBlock::~RemoteWritableBlock() {
}

Status RemoteWritableBlock::close() {
    return Status::IOError("invalid function", 0, "");
}

Status RemoteWritableBlock::abort() {
    return Status::IOError("invalid function", 0, "");
}

BlockManager* RemoteWritableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& RemoteWritableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const FilePathDesc& RemoteWritableBlock::path_desc() const {
    return _path_desc;
}

Status RemoteWritableBlock::append(const Slice& data) {
    return appendv(&data, 1);
}

Status RemoteWritableBlock::appendv(const Slice* data, size_t data_cnt) {
    return Status::IOError("invalid function", 0, "");
}

Status RemoteWritableBlock::flush_data_async() {
    return Status::IOError("invalid function", 0, "");
}

Status RemoteWritableBlock::finalize() {
    return Status::IOError("invalid function", 0, "");
}

size_t RemoteWritableBlock::bytes_appended() const {
    return _bytes_appended;
}

WritableBlock::State RemoteWritableBlock::state() const {
    return _state;
}

Status RemoteWritableBlock::_close(SyncMode mode) {
    return Status::IOError("invalid function", 0, "");
}

////////////////////////////////////////////////////////////
// RemoteReadableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for reading.
//
// There may be millions of instances of RemoteReadableBlock outstanding, so
// great care must be taken to reduce its size. To that end, it does _not_
// embed a FileBlockLocation, using the simpler BlockId instead.
class RemoteReadableBlock : public ReadableBlock {
public:
    RemoteReadableBlock(RemoteBlockManager* block_manager, const FilePathDesc& path_desc,
                        std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle,
                        CachedSegmentCacheHandle* cache_handle);

    virtual ~RemoteReadableBlock();

    virtual Status close() override;

    virtual BlockManager* block_manager() const override;

    virtual const BlockId& id() const override;
    virtual const FilePathDesc& path_desc() const override;

    virtual Status size(uint64_t* sz) const override;

    virtual Status read(uint64_t offset, Slice result) const override;

    virtual Status readv(uint64_t offset, const Slice* results, size_t res_cnt) const override;

    void handle_error(const Status& s) const;

private:
    // Back pointer to the owning block manager.
    RemoteBlockManager* _block_manager;

    // The block's identifier.
    const BlockId _block_id;
    const FilePathDesc _path_desc;

    // make sure this handle is initialized and valid before
    // reading data.
    CachedSegmentCacheHandle* _cached_segment_cache_handle;

    // The underlying opened file backing this block.
    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> _file_handle;
    // the backing file of OpenedFileHandle, not owned.
    RandomAccessFile* _file = nullptr;

    // Whether or not this block has been closed. Close() is thread-safe, so
    // this must be an atomic primitive.
    std::atomic_bool _closed;

    DISALLOW_COPY_AND_ASSIGN(RemoteReadableBlock);
};

RemoteReadableBlock::RemoteReadableBlock(
        RemoteBlockManager* block_manager, const FilePathDesc& path_desc,
        std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle,
        CachedSegmentCacheHandle* cache_handle)
        : _block_manager(block_manager),
          _path_desc(path_desc),
          _cached_segment_cache_handle(cache_handle),
          _file_handle(std::move(file_handle)),
          _closed(false) {
        _file = _file_handle->file();
}

RemoteReadableBlock::~RemoteReadableBlock() {
}

Status RemoteReadableBlock::close() {
    return Status::IOError("invalid function", 0, "");
}

BlockManager* RemoteReadableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& RemoteReadableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const FilePathDesc& RemoteReadableBlock::path_desc() const {
    return _path_desc;
}

Status RemoteReadableBlock::size(uint64_t* sz) const {
    DCHECK(!_closed.load());

    std::shared_lock<std::shared_mutex> segment_rdlock(_block_manager->get_segment_lock_ptr());
    RETURN_IF_ERROR(_file->size(sz));
    return Status::OK();
}

Status RemoteReadableBlock::read(uint64_t offset, Slice result) const {
    return readv(offset, &result, 1);
}

Status RemoteReadableBlock::readv(uint64_t offset, const Slice* results, size_t res_cnt) const {
    DCHECK(!_closed.load());

    std::shared_lock<std::shared_mutex> segment_rdlock(_block_manager->get_segment_lock_ptr());
    RETURN_IF_ERROR(_file->readv_at(offset, results, res_cnt));

    return Status::OK();
}

} // namespace internal

////////////////////////////////////////////////////////////
// RemoteBlockManager
////////////////////////////////////////////////////////////

RemoteBlockManager::RemoteBlockManager(Env* local_env, std::shared_ptr<StorageBackend> storage_backend,
                                       const BlockManagerOptions& opts)
        : _local_env(local_env), _storage_backend(storage_backend), _opts(opts) {
#ifdef BE_TEST
    _file_cache.reset(new FileCache<RandomAccessFile>("Readable_remote_cache",
            config::file_descriptor_cache_capacity));
#else
    _file_cache.reset(new FileCache<RandomAccessFile>("Readable_remote_cache",
            StorageEngine::instance()->file_cache()));
#endif
}

RemoteBlockManager::~RemoteBlockManager() {}

Status RemoteBlockManager::open() {
    return Status::NotSupported("to be implemented. (TODO)");
}

Status RemoteBlockManager::create_block(const CreateBlockOptions& opts,
                                        std::unique_ptr<WritableBlock>* block) {
    if (_opts.read_only) {
        std::stringstream ss;
        ss << "create_block failed. remote block is readonly: " << opts.path_desc.debug_string();
        return Status::NotSupported(ss.str());
    }

    shared_ptr<WritableFile> local_writer;
    WritableFileOptions wr_opts;
    wr_opts.mode = Env::MUST_CREATE;
    RETURN_IF_ERROR(env_util::open_file_for_write(
            wr_opts, Env::Default(), opts.path_desc.filepath, &local_writer));

    VLOG_CRITICAL << "Creating new remote block. local: " << opts.path_desc.filepath
                  << ", remote: " << opts.path_desc.remote_path;
    block->reset(new internal::RemoteWritableBlock(this, opts.path_desc, local_writer));
    return Status::OK();
}

Status RemoteBlockManager::open_block(const FilePathDesc& path_desc, std::unique_ptr<ReadableBlock>* block) {
    VLOG_CRITICAL << "Opening remote block. path_desc: " << path_desc.debug_string();
    if (!path_desc.is_remote()) {
        return Status::OLAPInternalError(OLAP_ERR_STORAGE_NOT_REMOTE);
    }

    // If local cache flag file `filepath_done` dose not exist, download segments first.
    string file_path = path_desc.filepath;
    string download_file_path = file_path + "_done";

    // load cached segments
    CachedSegmentCacheHandle cache_handle;
    bool cached = StorageEngine::instance()->remote_file_cache()->load_cached_segment(
            download_file_path, &cache_handle);

    // If the segment is not cached, download it first, then insert it into cache.
    if (!cached) {
        std::shared_ptr<StorageBackend> storage_backend = StorageBackendMgr::instance()->get_storage_backend(path_desc.storage_name);
        if (storage_backend == nullptr) {
            return Status::OLAPInternalError(OLAP_ERR_STORAGE_BACKEND_NOT_EXIST);
        }
        // add write lock to check done flag file's existence
        std::lock_guard<std::shared_mutex> segment_wrlock(_segment_lock);
        if (!Env::Default()->path_exists(download_file_path).ok()) {
            Status status = storage_backend->download(path_desc.remote_path, file_path);
            if (!status.ok()) {
                LOG(WARNING) << "fail to download file. from=" << path_desc.remote_path << ", to="
                             << file_path << ", error_msg=" << status.get_error_msg();
                return status;
            }

            // create download done file
            Status st = FileSystemUtil::create_file(download_file_path);
            if (!st.ok()) {
                LOG(WARNING) << "fail to create download_done file[" << download_file_path
                             << "], with error: " << st.get_error_msg();
                return st;
            }

            LOG(INFO) << "Download file successfully. from="<< path_desc.remote_path << ", to="
                      << file_path;
        }
        // insert into cache
        StorageEngine::instance()->remote_file_cache()->insert(download_file_path, file_path, &cache_handle);
    }

    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle;
    file_handle.reset(new OpenedFileHandle<RandomAccessFile>());
    bool found = _file_cache->lookup(path_desc.filepath, file_handle.get());
    if (!found) {
        std::unique_ptr<RandomAccessFile> file;
        RETURN_IF_ERROR(Env::Default()->new_random_access_file(path_desc.filepath, &file));
        _file_cache->insert(path_desc.filepath, file.release(), file_handle.get());
    }
    block->reset(new internal::RemoteReadableBlock(this, path_desc, file_handle, &cache_handle));
    return Status::OK();
}

Status RemoteBlockManager::delete_block(const FilePathDesc& path_desc, bool is_dir) {
    if (is_dir) {
        if (_local_env->path_exists(path_desc.filepath).ok()) {
            RETURN_IF_ERROR(_local_env->delete_dir(path_desc.filepath));
        }
        if (!path_desc.remote_path.empty()) {
            RETURN_IF_ERROR(_storage_backend->rmdir(path_desc.remote_path));
        }
    } else {
        if (_local_env->path_exists(path_desc.filepath).ok()) {
            RETURN_IF_ERROR(_local_env->delete_file(path_desc.filepath));
        }
        if (_storage_backend->exist(path_desc.remote_path).ok()) {
            RETURN_IF_ERROR(_storage_backend->rm(path_desc.remote_path));
        }
    }
    return Status::OK();
}

Status RemoteBlockManager::link_file(const FilePathDesc& src_path_desc, const FilePathDesc& dest_path_desc) {
    if (_local_env->path_exists(src_path_desc.filepath).ok()) {
        RETURN_IF_ERROR(_local_env->link_file(src_path_desc.filepath, dest_path_desc.filepath));
    }
    if (_storage_backend->exist(src_path_desc.remote_path).ok()) {
        RETURN_IF_ERROR(_storage_backend->copy(src_path_desc.remote_path, dest_path_desc.remote_path));
    }
    return Status::OK();
}

} // namespace fs
} // namespace doris
