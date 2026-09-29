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

#include "storage/index/snii/snii_blob_directory.h"

#include <fmt/format.h>

#include <utility>

#include "common/check.h"

namespace doris::segment_v2::snii_doris {
namespace {

// IndexInput over one blob sub-file. readInternal fills the caller's buffer
// through DorisSniiFileReader::read_into -- one positional read, no vector
// round-trip -- so BufferedIndexInput's large-read bypass (len >= bufferSize
// goes straight to the destination) costs no extra resident copy. read_into is
// stateless, so clones share nothing but the reader handle: no base-stream
// mutex (unlike CSIndexInput, whose shared base cursor needs one).
class SniiBlobIndexInput final : public lucene::store::BufferedIndexInput {
public:
    SniiBlobIndexInput(std::shared_ptr<DorisSniiFileReader> reader, uint64_t blob_offset,
                       int64_t blob_length, int32_t buffer_size)
            : BufferedIndexInput(buffer_size),
              _reader(std::move(reader)),
              _blob_offset(blob_offset),
              _blob_length(blob_length) {}

    // A clone deliberately does NOT inherit _io_ctx (it stays null), matching
    // CSIndexInput. The context is a borrowed, non-owning pointer whose lifetime
    // is the caller's stack frame; a clone can outlive it (bkd_reader keeps
    // per-intersect-state clones), so inheriting it would leave a dangling
    // pointer that readInternal would dereference. Consumers that need a context
    // on a clone set it explicitly -- bkd_reader's intersect_state does exactly
    // that right after cloning.
    SniiBlobIndexInput(const SniiBlobIndexInput& other)
            : BufferedIndexInput(other),
              _reader(other._reader),
              _blob_offset(other._blob_offset),
              _blob_length(other._blob_length) {}

    lucene::store::IndexInput* clone() const override { return _CLNEW SniiBlobIndexInput(*this); }
    void close() override {}
    int64_t length() const override { return _blob_length; }
    const char* getDirectoryType() const override { return SniiBlobDirectory::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }
    static const char* getClassName() { return "SniiBlobIndexInput"; }
    void setIoContext(const void* io_ctx) override {
        _io_ctx = static_cast<const io::IOContext*>(io_ctx);
    }

protected:
    void readInternal(uint8_t* b, const int32_t len) override {
        const int64_t start = getFilePointer();
        if (len < 0 || start > _blob_length || len > _blob_length - start) {
            _CLTHROWA(CL_ERR_IO, "read past EOF");
        }
        // IOContext is a thread-local SCOPE on the SNII read stack, not a
        // parameter: an explicitly injected context is pushed around this one
        // read; otherwise the reader's own default (INDEX-partition) context
        // applies inside read_into.
        Status status;
        if (_io_ctx != nullptr) {
            DorisSniiFileReader::ScopedIOContext scoped(_io_ctx);
            status = _reader->read_into(_blob_offset + static_cast<uint64_t>(start), b,
                                        static_cast<size_t>(len));
        } else {
            status = _reader->read_into(_blob_offset + static_cast<uint64_t>(start), b,
                                        static_cast<size_t>(len));
        }
        if (!status.ok()) {
            // CLuceneError STRDUPs the message; the temporary is safe.
            _CLTHROWA(CL_ERR_IO, status.to_string().c_str());
        }
    }
    void seekInternal(const int64_t /*pos*/) override {}

private:
    std::shared_ptr<DorisSniiFileReader> _reader;
    uint64_t _blob_offset;
    int64_t _blob_length;
    const io::IOContext* _io_ctx = nullptr;
};

} // namespace

Status SniiBlobDirectory::open(std::shared_ptr<DorisSniiFileReader> reader,
                               const snii::format::LogicalIndexMetadataRef& entry,
                               uint64_t data_area_end, SniiBlobDirectoryPtr* out,
                               int32_t read_buffer_size) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("blob directory: null out");
    }
    out->reset();
    if (reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("blob directory: null reader");
    }
    if (entry.kind == snii::format::LogicalIndexKind::kInverted) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "blob directory: entry is a text inverted index");
    }
    if (entry.files.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "blob directory: entry has no files");
    }
    if (data_area_end > reader->size()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                fmt::format("blob directory: data area end {} exceeds container size {}",
                            data_area_end, reader->size()));
    }
    for (const snii::format::NamedBlobFileRef& file : entry.files) {
        if (file.offset > data_area_end || file.length > data_area_end - file.offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(fmt::format(
                    "blob directory: file {} [{}, +{}) is outside the container data area of "
                    "{} bytes",
                    file.name, file.offset, file.length, data_area_end));
        }
    }
    if (read_buffer_size <= 0) {
        read_buffer_size = lucene::store::BufferedIndexInput::BUFFER_SIZE;
    }
    out->reset(new SniiBlobDirectory(std::move(reader), entry, read_buffer_size));
    return Status::OK();
}

SniiBlobDirectory::SniiBlobDirectory(std::shared_ptr<DorisSniiFileReader> reader,
                                     snii::format::LogicalIndexMetadataRef entry,
                                     int32_t read_buffer_size)
        : _reader(std::move(reader)),
          _entry(std::move(entry)),
          _read_buffer_size(read_buffer_size) {}

SniiBlobDirectory::~SniiBlobDirectory() = default;

const snii::format::NamedBlobFileRef* SniiBlobDirectory::find_file(const char* name) const {
    // A null name is a caller bug, not an absent file: returning "absent" would
    // hide it, and every caller then formats `name` into its error message.
    DORIS_CHECK(name != nullptr);
    for (const snii::format::NamedBlobFileRef& file : _entry.files) {
        if (file.name == name) {
            return &file;
        }
    }
    return nullptr;
}

bool SniiBlobDirectory::list(std::vector<std::string>* names) const {
    DORIS_CHECK(names != nullptr);
    for (const snii::format::NamedBlobFileRef& file : _entry.files) {
        names->push_back(file.name);
    }
    return true;
}

bool SniiBlobDirectory::fileExists(const char* name) const {
    return find_file(name) != nullptr;
}

int64_t SniiBlobDirectory::fileModified(const char* /*name*/) const {
    return 0;
}

int64_t SniiBlobDirectory::fileLength(const char* name) const {
    const snii::format::NamedBlobFileRef* file = find_file(name);
    if (file == nullptr) {
        const std::string message = fmt::format("File does not exist in SNII blob entry: {}", name);
        _CLTHROWA(CL_ERR_IO, message.c_str()); // CLuceneError STRDUPs the message
    }
    return static_cast<int64_t>(file->length);
}

bool SniiBlobDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                  CLuceneError& err, int32_t bufferSize) {
    ret = nullptr;
    if (_closed) {
        err.set(CL_ERR_IO, "SniiBlobDirectory is already closed");
        return false;
    }
    const snii::format::NamedBlobFileRef* file = find_file(name);
    if (file == nullptr) {
        // Report-and-return rather than throw, matching DorisCompoundReader: this
        // is the 4-arg overload's contract. Note that CLucene's 2-arg
        // Directory::openInput re-throws this err (Directory.cpp), and
        // bkd_reader::open() uses THAT overload -- so a genuinely missing file
        // still surfaces as a CLuceneError to the searcher, which is correct: a
        // missing sub-file is an error. It is a ZERO-LENGTH sub-file, handled
        // below, that must not be confused with corruption.
        err.set(CL_ERR_IO, fmt::format("SNII blob entry (index_id={}, suffix={}) has no file '{}'",
                                       _entry.index_id, _entry.index_suffix, name)
                                   .c_str());
        return false;
    }
    if (bufferSize <= 0) {
        bufferSize = _read_buffer_size;
    }
    // A 0-length entry yields a length()==0 input on purpose: an empty BKD
    // segment stores 0-byte `bkd` / `bkd_index` files, and throwing here (the
    // FSIndexInput EmptyIndexSegment behavior) would turn "empty" into
    // "corrupt" (design 2026-07-28 §3.4).
    ret = _CLNEW SniiBlobIndexInput(_reader, file->offset, static_cast<int64_t>(file->length),
                                    bufferSize);
    return true;
}

void SniiBlobDirectory::renameFile(const char* /*from*/, const char* /*to*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobDirectory::renameFile");
}

void SniiBlobDirectory::touchFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobDirectory::touchFile");
}

lucene::store::IndexOutput* SniiBlobDirectory::createOutput(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobDirectory::createOutput");
}

bool SniiBlobDirectory::doDeleteFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobDirectory::doDeleteFile");
}

// Never throws: ~bkd_reader (implicitly noexcept) calls close() whenever it
// owns the directory, with no try/catch anywhere on that path. Both statements
// below are noexcept. Releasing the reader here (rather than at destruction)
// drops this directory's hold on the segment file handle at the point the
// caller asked for it; already-opened inputs keep their own shared_ptr and stay
// valid, exactly as CLucene expects.
void SniiBlobDirectory::close() {
    _closed = true;
    _reader.reset();
}

std::string SniiBlobDirectory::toString() const {
    return fmt::format("SniiBlobDirectory(index_id={}, suffix={})", _entry.index_id,
                       _entry.index_suffix);
}

const char* SniiBlobDirectory::getClassName() {
    return "SniiBlobDirectory";
}

const char* SniiBlobDirectory::getObjectName() const {
    return getClassName();
}

} // namespace doris::segment_v2::snii_doris
