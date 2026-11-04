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

#include "storage/index/inverted/snii/snii_ann_store.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/check.h"
#include "common/logging.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "storage/index/inverted/snii/bkd/staged_blob_file.h"
#include "storage/index/inverted/snii/common/slice.h"
#include "storage/index/inverted/snii/snii_doris_adapter.h"

namespace doris::segment_v2::snii_doris {

namespace {

// One sub-file of an ANN blob. Its reads go to the container's reader at the sub-file's offset,
// through a buffer that works like CLucene's BufferedIndexInput, which V1 and V2 read through: the
// callers coalesce nothing (see AnnIndexInput::read_at), so without it faiss::read_index would
// send every 8-byte field of the index to the file as a read of its own.
//
// A read the buffer holds whole is a copy. Otherwise the buffer serves the part it holds, and the
// rest is read into the buffer from where it starts, buffer-size bytes or up to the end of the
// sub-file; only a rest at least as large as the buffer goes straight into the caller's memory.
// With a 4 KiB buffer, faiss reading a fourcc, a dimension and a count at offsets 0, 4 and 8
// costs one read of [0, 4096), and an IVF list of 3840 bytes of codes at 8192 costs one read of
// [8192, 12288), which also holds the 240 bytes of ids that follow them.
class SniiAnnInput final : public AnnIndexInput {
public:
    SniiAnnInput(std::shared_ptr<DorisSniiFileReader> reader, snii::format::NamedBlobFileRef file,
                 size_t buffer_size)
            : _reader(std::move(reader)), _file(std::move(file)), _buffer_size(buffer_size) {}

    uint64_t length() const override { return _file.length; }

    Status read_at(uint64_t offset, void* out, size_t size, const io::IOContext* io_ctx) override {
        // The container's reader bounds a read by the container alone. The sub-file's length comes
        // from the directory, so past it a read would serve the next blob as this file's tail.
        if (offset > _file.length || size > _file.length - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "Failed to read {} bytes at offset {} of {}, which holds {} bytes", size,
                    offset, _file.name, _file.length);
        }
        auto* bytes = static_cast<uint8_t*>(out);
        const uint64_t buffered_end = _buffer_start + _buffer_length;
        if (offset >= _buffer_start && offset + size <= buffered_end) {
            std::memcpy(bytes, _buffer.data() + (offset - _buffer_start), size);
            return Status::OK();
        }
        if (offset >= _buffer_start && offset < buffered_end) {
            const size_t buffered = buffered_end - offset;
            std::memcpy(bytes, _buffer.data() + (offset - _buffer_start), buffered);
            bytes += buffered;
            offset += buffered;
            size -= buffered;
        }
        if (size >= _buffer_size) {
            // Emptied like BufferedIndexInput's after a read past it, so both send the file the
            // same reads whatever comes next.
            _buffer_length = 0;
            return _read_file(offset, bytes, size, io_ctx);
        }
        const size_t refill = std::min<uint64_t>(_buffer_size, _file.length - offset);
        DCHECK_LE(size, refill);
        _buffer.resize(_buffer_size);
        // Empty until the refill succeeds: a failed read leaves no stale bytes to serve.
        _buffer_length = 0;
        RETURN_IF_ERROR(_read_file(offset, _buffer.data(), refill, io_ctx));
        _buffer_start = offset;
        _buffer_length = refill;
        std::memcpy(bytes, _buffer.data(), size);
        return Status::OK();
    }

private:
    // One read of the file, charged to the caller's context for this call only.
    Status _read_file(uint64_t offset, uint8_t* out, size_t size,
                      const io::IOContext* io_ctx) const {
        // An ANN file is index data, not metadata, whatever the context says: is_index_data stays
        // false, as FSIndexInput kept it for these files.
        io::IOContext read_ctx = io_ctx == nullptr ? io::IOContext {} : *io_ctx;
        read_ctx.is_index_data = false;
        DorisSniiFileReader::ScopedIOContext scope(&read_ctx);
        int64_t io_ns = 0;
        Status status;
        {
            SCOPED_RAW_TIMER(&io_ns);
            status = _reader->read_into(_file.offset + offset, out, size);
        }
        // The reader counts the read and its bytes. Its time is recorded here, as FSIndexInput
        // recorded it, so the profile's InvertedIndexIOTimer still covers ANN reads.
        if (read_ctx.file_cache_stats != nullptr) {
            read_ctx.file_cache_stats->inverted_index_io_timer += io_ns;
        }
        if (status.is<ErrorCode::NOT_FOUND>()) {
            // A missing file is an I/O failure here: index callers take NOT_FOUND for an index
            // that is not there and fall back, and an ANN read has no fallback.
            return Status::Error<ErrorCode::IO_ERROR, false>(
                    "Failed to read {} bytes at offset {} of {}: {}", size, offset, _file.name,
                    status.msg());
        }
        if (!status.ok()) {
            status.prepend(fmt::format("Failed to read {} bytes at offset {} of {}: ", size, offset,
                                       _file.name));
        }
        return status;
    }

    const std::shared_ptr<DorisSniiFileReader> _reader;
    // The name, the absolute offset in the container and the length.
    const snii::format::NamedBlobFileRef _file;
    const size_t _buffer_size;
    // Allocated on the first refill. Holds the bytes [_buffer_start, _buffer_start +
    // _buffer_length) of the sub-file.
    std::vector<uint8_t> _buffer;
    uint64_t _buffer_start = 0;
    size_t _buffer_length = 0;
};

// Appends one staged file. StagedBlobFile::append is a write(2) of what it is given, and faiss
// serializes in calls of a few bytes, so the output buffers 64 KiB, the size CLucene's
// BufferedIndexOutput buffered when the staging was a CLucene directory. The complete payload
// still stays out of the process heap.
class SniiAnnStagingOutput final : public AnnIndexOutput {
public:
    SniiAnnStagingOutput(std::string name, std::shared_ptr<snii::bkd::StagedBlobFile> file)
            : _name(std::move(name)), _file(std::move(file)), _buffer(kBufferBytes) {}

    // Still open only when the serialization failed, and that failure is already on its way back.
    ~SniiAnnStagingOutput() override {
        if (Status status = SniiAnnStagingOutput::close(); !status.ok()) {
            LOG(WARNING) << "Failed to close an ANN staging output left open by a failed write: "
                         << status.msg();
        }
    }

    Status append(const void* data, size_t size) override {
        DCHECK(!_closed) << _name;
        Status status = _append(static_cast<const uint8_t*>(data), size);
        if (!status.ok()) {
            status.prepend(fmt::format("Failed to write {}: ", _name));
        }
        return status;
    }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        // Marked before the attempt: a failed close is not retried, so the staged file is never
        // finalized twice. Discarding the staging unlinks it either way.
        _closed = true;
        Status status = _flush();
        if (status.ok()) {
            status = _file->finalize();
        }
        if (!status.ok()) {
            status.prepend(fmt::format("Failed to close {}: ", _name));
        }
        return status;
    }

private:
    static constexpr size_t kBufferBytes = 64 * 1024;

    // Bytes the buffer has room for are copied. Otherwise what it holds goes out first, and the
    // new bytes then fill it anew, or go straight to the file when they would fill it whole.
    Status _append(const uint8_t* bytes, size_t size) {
        // faiss writes an empty vector as its data() and a size of 0, and data() of an empty
        // std::vector is null. memcpy takes no null pointer even for 0 bytes (UBSan reports it),
        // so an empty append stops here, as it never reached CLucene's output either.
        if (size == 0) {
            return Status::OK();
        }
        if (size <= kBufferBytes - _buffered) {
            std::memcpy(_buffer.data() + _buffered, bytes, size);
            _buffered += size;
            return Status::OK();
        }
        RETURN_IF_ERROR(_flush());
        if (size >= kBufferBytes) {
            return _file->append(snii::Slice(bytes, size));
        }
        std::memcpy(_buffer.data(), bytes, size);
        _buffered = size;
        return Status::OK();
    }

    Status _flush() {
        // Nothing buffered, nothing to write: a large append right after a flush, or a close right
        // after one, has no bytes for the file.
        if (_buffered == 0) {
            return Status::OK();
        }
        const size_t buffered = std::exchange(_buffered, 0);
        return _file->append(snii::Slice(_buffer.data(), buffered));
    }

    const std::string _name;
    const std::shared_ptr<snii::bkd::StagedBlobFile> _file;
    std::vector<uint8_t> _buffer;
    size_t _buffered = 0;
    bool _closed = false;
};

} // namespace

SniiAnnSource::SniiAnnSource(std::shared_ptr<DorisSniiFileReader> reader,
                             snii::format::LogicalIndexMetadataRef entry, size_t read_buffer_size)
        : _reader(std::move(reader)),
          _entry(std::move(entry)),
          _read_buffer_size(read_buffer_size) {
    DORIS_CHECK(_reader != nullptr);
    DORIS_CHECK(_entry.kind == snii::format::LogicalIndexKind::kAnn);
}

Result<std::unique_ptr<AnnIndexInput>> SniiAnnSource::open_input(std::string_view name) {
    const auto file = std::ranges::find(_entry.files, name, &snii::format::NamedBlobFileRef::name);
    if (file == _entry.files.end()) {
        // faiss opens only the files its index type writes, so the blob of an index of that type
        // lacks one only when the container is damaged.
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "Failed to open index file: {}, error: {} has no such file", name, debug_string()));
    }
    return std::make_unique<SniiAnnInput>(_reader, *file, _read_buffer_size);
}

std::string SniiAnnSource::debug_string() const {
    return fmt::format("SniiAnnSource(index_id={}, suffix={})", _entry.index_id,
                       _entry.index_suffix);
}

SniiAnnStaging::~SniiAnnStaging() = default;

Result<std::unique_ptr<AnnIndexOutput>> SniiAnnStaging::create_output(std::string_view name) {
    std::string file_name(name);
    std::unique_ptr<snii::bkd::StagedBlobFile> created;
    // Named after the file, so a staged file is recognizable on the temp filesystem.
    if (Status status = snii::bkd::StagedBlobFile::create(file_name, &created); !status.ok()) {
        status.prepend(fmt::format("Failed to create {} in {}: ", file_name, debug_string()));
        return ResultError(std::move(status));
    }
    auto file = std::shared_ptr<snii::bkd::StagedBlobFile>(std::move(created));
    // Replaced rather than reused: a blob source already taken over the old file keeps reading
    // the old bytes instead of seeing them change underneath.
    _files.insert_or_assign(file_name, file);
    return std::make_unique<SniiAnnStagingOutput>(std::move(file_name), std::move(file));
}

std::string SniiAnnStaging::debug_string() const {
    return fmt::format("SniiAnnStaging(files={}, bytes={})", _files.size(), staged_bytes());
}

std::vector<snii::writer::BlobFileSource> SniiAnnStaging::take_blob_sources() {
    // Moved out before anything is built from it: a source has to be the file's only owner, or
    // the per-blob release in SniiCompoundWriter::finish() frees nothing and the staging survives
    // until this object does.
    std::map<std::string, std::shared_ptr<snii::bkd::StagedBlobFile>> taken;
    taken.swap(_files);
    std::vector<snii::writer::BlobFileSource> sources;
    sources.reserve(taken.size());
    // std::map iterates in name order, the order the container must seal the sub-files in.
    for (auto& [name, staged] : taken) {
        auto file = std::move(staged);
        const uint64_t length = file->bytes_written();
        sources.push_back(snii::writer::BlobFileSource {
                .name = name,
                .length = length,
                .read_fn = [file = std::move(file)](uint64_t offset, size_t len, uint8_t* out)
                        -> Status { return file->read_at(offset, len, out); }});
    }
    return sources;
}

void SniiAnnStaging::discard_staged_files() {
    // Same swap-with-empty as take_blob_sources(): it is the shared_ptr elements themselves that
    // unlink the files.
    std::map<std::string, std::shared_ptr<snii::bkd::StagedBlobFile>> dropped;
    dropped.swap(_files);
}

uint64_t SniiAnnStaging::staged_bytes() const {
    uint64_t total = 0;
    for (const auto& [name, file] : _files) {
        total += file->bytes_written();
    }
    return total;
}

} // namespace doris::segment_v2::snii_doris
