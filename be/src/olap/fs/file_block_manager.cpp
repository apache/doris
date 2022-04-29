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

#include "olap/fs/file_block_manager.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "env/env_util.h"
#include "gutil/strings/substitute.h"
#include "olap/fs/block_id.h"
#include "olap/fs/block_manager_metrics.h"
#include "olap/storage_engine.h"
#include "util/doris_metrics.h"
#include "util/file_cache.h"
#include "util/metrics.h"
#include "util/path_util.h"
#include "util/slice.h"

using std::accumulate;
using std::shared_ptr;
using std::string;

using strings::Substitute;

namespace doris {
namespace fs {

namespace internal {

////////////////////////////////////////////////////////////
// FileWritableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for writing.
//
// Contains a pointer to the block manager as well as file path
// so that dirty metadata can be synced via BlockManager::SyncMetadata()
// at Close() time. Embedding a file path (and not a simpler
// BlockId) consumes more memory, but the number of outstanding
// FileWritableBlock instances is expected to be low.
class FileWritableBlock : public WritableBlock {
public:
    FileWritableBlock(FileBlockManager* block_manager, const FilePathDesc& path_desc,
                      shared_ptr<WritableFile> writer);

    virtual ~FileWritableBlock();

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
    DISALLOW_COPY_AND_ASSIGN(FileWritableBlock);

    enum SyncMode { SYNC, NO_SYNC };

    // Close the block, optionally synchronizing dirty data and metadata.
    Status _close(SyncMode mode);

    // Back pointer to the block manager.
    //
    // Should remain alive for the lifetime of this block.
    FileBlockManager* _block_manager;

    const BlockId _block_id;
    FilePathDesc _path_desc;

    // The underlying opened file backing this block.
    shared_ptr<WritableFile> _writer;

    State _state;

    // The number of bytes successfully appended to the block.
    size_t _bytes_appended;
};

FileWritableBlock::FileWritableBlock(FileBlockManager* block_manager, const FilePathDesc& path_desc,
                                     shared_ptr<WritableFile> writer)
        : _block_manager(block_manager),
          _path_desc(path_desc),
          _writer(writer),
          _state(CLEAN),
          _bytes_appended(0) {
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_writing->increment(1);
        _block_manager->_metrics->total_writable_blocks->increment(1);
    }
}

FileWritableBlock::~FileWritableBlock() {
    if (_state != CLOSED) {
        WARN_IF_ERROR(abort(),
                      strings::Substitute("Failed to close block $0", _path_desc.filepath));
    }
}

Status FileWritableBlock::close() {
    return _close(SYNC);
}

Status FileWritableBlock::abort() {
    RETURN_IF_ERROR(_close(NO_SYNC));
    return _block_manager->delete_block(_path_desc);
}

BlockManager* FileWritableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& FileWritableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const FilePathDesc& FileWritableBlock::path_desc() const {
    return _path_desc;
}

Status FileWritableBlock::append(const Slice& data) {
    return appendv(&data, 1);
}

Status FileWritableBlock::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(_state == CLEAN || _state == DIRTY)
            << "path=" << _path_desc.filepath << " invalid state=" << _state;
    RETURN_IF_ERROR(_writer->appendv(data, data_cnt));
    _state = DIRTY;

    // Calculate the amount of data written
    size_t bytes_written =
            accumulate(data, data + data_cnt, static_cast<size_t>(0),
                       [](size_t sum, const Slice& curr) { return sum + curr.size; });
    _bytes_appended += bytes_written;
    return Status::OK();
}

Status FileWritableBlock::flush_data_async() {
    VLOG_NOTICE << "Flushing block " << _path_desc.filepath;
    RETURN_IF_ERROR(_writer->flush(WritableFile::FLUSH_ASYNC));
    return Status::OK();
}

Status FileWritableBlock::finalize() {
    DCHECK(_state == CLEAN || _state == DIRTY || _state == FINALIZED)
            << "path=" << _path_desc.filepath << "Invalid state: " << _state;

    if (_state == FINALIZED) {
        return Status::OK();
    }
    VLOG_NOTICE << "Finalizing block " << _path_desc.filepath;
    if (_state == DIRTY && BlockManager::block_manager_preflush_control == "finalize") {
        flush_data_async();
    }
    _state = FINALIZED;
    return Status::OK();
}

size_t FileWritableBlock::bytes_appended() const {
    return _bytes_appended;
}

WritableBlock::State FileWritableBlock::state() const {
    return _state;
}

Status FileWritableBlock::_close(SyncMode mode) {
    if (_state == CLOSED) {
        return Status::OK();
    }

    Status sync;
    if (mode == SYNC && (_state == CLEAN || _state == DIRTY || _state == FINALIZED)) {
        // Safer to synchronize data first, then metadata.
        VLOG_NOTICE << "Syncing block " << _path_desc.filepath;
        if (_block_manager->_metrics) {
            _block_manager->_metrics->total_disk_sync->increment(1);
        }
        sync = _writer->sync();
        if (sync.ok()) {
            sync = _block_manager->_sync_metadata(_path_desc.filepath);
        }
        WARN_IF_ERROR(sync, strings::Substitute("Failed to sync when closing block $0",
                                                _path_desc.filepath));
    }
    Status close = _writer->close();

    _state = CLOSED;
    _writer.reset();
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_writing->increment(-1);
        _block_manager->_metrics->total_bytes_written->increment(_bytes_appended);
        _block_manager->_metrics->total_blocks_created->increment(1);
    }

    // Either Close() or Sync() could have run into an error.
    RETURN_IF_ERROR(close);
    RETURN_IF_ERROR(sync);

    // Prefer the result of Close() to that of Sync().
    return close.ok() ? close : sync;
}

////////////////////////////////////////////////////////////
// FileReadableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for reading.
//
// There may be millions of instances of FileReadableBlock outstanding, so
// great care must be taken to reduce its size. To that end, it does _not_
// embed a FileBlockLocation, using the simpler BlockId instead.
class FileReadableBlock : public ReadableBlock {
public:
    FileReadableBlock(FileBlockManager* block_manager, const FilePathDesc& path_desc,
                      std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle);

    virtual ~FileReadableBlock();

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
    FileBlockManager* _block_manager;

    // The block's identifier.
    const BlockId _block_id;
    const FilePathDesc _path_desc;

    // The underlying opened file backing this block.
    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> _file_handle;
    // the backing file of OpenedFileHandle, not owned.
    RandomAccessFile* _file;

    // Whether or not this block has been closed. Close() is thread-safe, so
    // this must be an atomic primitive.
    std::atomic_bool _closed;

    DISALLOW_COPY_AND_ASSIGN(FileReadableBlock);
};

FileReadableBlock::FileReadableBlock(
        FileBlockManager* block_manager, const FilePathDesc& path_desc,
        std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle)
        : _block_manager(block_manager),
          _path_desc(path_desc),
          _file_handle(std::move(file_handle)),
          _closed(false) {
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_reading->increment(1);
        _block_manager->_metrics->total_readable_blocks->increment(1);
    }
    _file = _file_handle->file();
}

FileReadableBlock::~FileReadableBlock() {
    WARN_IF_ERROR(close(), strings::Substitute("Failed to close block $0", _path_desc.filepath));
}

Status FileReadableBlock::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true)) {
        _file_handle.reset();
        if (_block_manager->_metrics) {
            _block_manager->_metrics->blocks_open_reading->increment(-1);
        }
    }

    return Status::OK();
}

BlockManager* FileReadableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& FileReadableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const FilePathDesc& FileReadableBlock::path_desc() const {
    return _path_desc;
}

Status FileReadableBlock::size(uint64_t* sz) const {
    DCHECK(!_closed.load());

    RETURN_IF_ERROR(_file->size(sz));
    return Status::OK();
}

Status FileReadableBlock::read(uint64_t offset, Slice result) const {
    return readv(offset, &result, 1);
}

Status FileReadableBlock::readv(uint64_t offset, const Slice* results, size_t res_cnt) const {
    DCHECK(!_closed.load());

    RETURN_IF_ERROR(_file->readv_at(offset, results, res_cnt));

    if (_block_manager->_metrics) {
        // Calculate the read amount of data
        size_t bytes_read = accumulate(results, results + res_cnt, static_cast<size_t>(0),
                                       [&](int sum, const Slice& curr) { return sum + curr.size; });
        _block_manager->_metrics->total_bytes_read->increment(bytes_read);
    }

    return Status::OK();
}

} // namespace internal

////////////////////////////////////////////////////////////
// FileBlockManager
////////////////////////////////////////////////////////////

FileBlockManager::FileBlockManager(Env* env, BlockManagerOptions opts)
        : _env(DCHECK_NOTNULL(env)), _opts(std::move(opts)) {
    if (_opts.enable_metric) {
        _metrics.reset(new internal::BlockManagerMetrics());
    }

#ifdef BE_TEST
    _file_cache.reset(new FileCache<RandomAccessFile>("Readable_file_cache",
                                                      config::file_descriptor_cache_capacity));
#else
    _file_cache.reset(new FileCache<RandomAccessFile>("Readable_file_cache",
                                                      StorageEngine::instance()->file_cache()));
#endif
}

FileBlockManager::~FileBlockManager() {}

Status FileBlockManager::open() {
    // TODO(lingbin)
    return Status::NotSupported("to be implemented. (TODO)");
}

Status FileBlockManager::create_block(const CreateBlockOptions& opts,
                                      std::unique_ptr<WritableBlock>* block) {
    CHECK(!_opts.read_only);

    shared_ptr<WritableFile> writer;
    WritableFileOptions wr_opts;
    wr_opts.mode = Env::MUST_CREATE;
    RETURN_IF_ERROR(env_util::open_file_for_write(wr_opts, _env, opts.path_desc.filepath, &writer));

    VLOG_CRITICAL << "Creating new block at " << opts.path_desc.filepath;
    block->reset(new internal::FileWritableBlock(this, opts.path_desc, writer));
    return Status::OK();
}

Status FileBlockManager::open_block(const FilePathDesc& path_desc,
                                    std::unique_ptr<ReadableBlock>* block) {
    VLOG_CRITICAL << "Opening block with path at " << path_desc.filepath;
    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle(
            new OpenedFileHandle<RandomAccessFile>());
    bool found = _file_cache->lookup(path_desc.filepath, file_handle.get());
    if (!found) {
        std::unique_ptr<RandomAccessFile> file;
        RETURN_IF_ERROR(_env->new_random_access_file(path_desc.filepath, &file));
        _file_cache->insert(path_desc.filepath, file.release(), file_handle.get());
    }

    block->reset(new internal::FileReadableBlock(this, path_desc, file_handle));
    return Status::OK();
}

// TODO(lingbin): We should do something to ensure that deletion can only be done
// after the last reader or writer has finished
Status FileBlockManager::delete_block(const FilePathDesc& path_desc, bool is_dir) {
    CHECK(!_opts.read_only);

    RETURN_IF_ERROR(_env->delete_file(path_desc.filepath));

    // We don't bother fsyncing the parent directory as there's nothing to be
    // gained by ensuring that the deletion is made durable. Even if we did
    // fsync it, we'd need to account for garbage at startup time (in the
    // event that we crashed just before the fsync), and with such accounting
    // fsync-as-you-delete is unnecessary.
    //
    // The block's directory hierarchy is left behind. We could prune it if
    // it's empty, but that's racy and leaving it isn't much overhead.

    return Status::OK();
}

Status FileBlockManager::link_file(const FilePathDesc& src_path_desc,
                                   const FilePathDesc& dest_path_desc) {
    if (link(src_path_desc.filepath.c_str(), dest_path_desc.filepath.c_str()) != 0) {
        LOG(WARNING) << "fail to create hard link. from=" << src_path_desc.filepath << ", "
                     << "to=" << dest_path_desc.filepath << ", "
                     << "errno=" << Errno::no();
        return Status::InternalError("link file failed");
    }
    return Status::OK();
}

// TODO(lingbin): only one level is enough?
Status FileBlockManager::_sync_metadata(const FilePathDesc& path_desc) {
    string dir = path_util::dir_name(path_desc.filepath);
    if (_metrics) {
        _metrics->total_disk_sync->increment(1);
    }
    RETURN_IF_ERROR(_env->sync_dir(dir));
    return Status::OK();
}

} // namespace fs
} // namespace doris
