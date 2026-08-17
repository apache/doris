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

#include "storage/index/snii/snii_blob_staging_directory.h"

#include <fmt/format.h>

#include <cstring>
#include <utility>

#include "common/check.h"
#include "common/status.h"

namespace doris::segment_v2::snii_doris {

// Appends into one staged buffer. BufferedIndexOutput already batches the
// writer's byte-at-a-time calls, so flushBuffer sees 64 KiB chunks and the
// buffer grows geometrically through vector::insert.
//
// The buffer is held by shared_ptr because a blob source handed to the container
// keeps it alive on its own: an output may be closed and destroyed, and the
// directory itself dropped, long before finish() pulls the bytes.
class SniiBlobStagingDirectory::StagingIndexOutput final
        : public lucene::store::BufferedIndexOutput {
public:
    explicit StagingIndexOutput(std::shared_ptr<Buffer> buffer) : _buffer(std::move(buffer)) {}

    ~StagingIndexOutput() override {
        // MUST close here, qualified. ~BufferedIndexOutput also calls close() if
        // the caller did not -- but by then this subobject is gone, so the call
        // resolves to BufferedIndexOutput::close() -> flush() -> the PURE virtual
        // flushBuffer, i.e. __cxa_pure_virtual and a BE abort. Closing first
        // frees the base buffer, which is exactly what stops the base destructor
        // from trying. Same reason DorisFSDirectory::FSIndexOutput does it.
        try {
            StagingIndexOutput::close();
        } catch (const CLuceneError&) {
            // A destructor may not throw. Nothing here can fail anyway --
            // flushBuffer only appends to a vector -- but the base close() is
            // declared throwing, so the guard has to exist.
        }
    }

    void close() override { BufferedIndexOutput::close(); }

    int64_t length() const override { return static_cast<int64_t>(_buffer->size()); }

protected:
    void flushBuffer(const uint8_t* b, const int32_t size) override {
        // flush() is also called with an empty buffer (on close, and on every
        // seek), which is not an error.
        if (b == nullptr || size <= 0) {
            return;
        }
        _buffer->insert(_buffer->end(), b, b + size);
    }

private:
    const std::shared_ptr<Buffer> _buffer;
};

SniiBlobStagingDirectory::~SniiBlobStagingDirectory() = default;

const char* SniiBlobStagingDirectory::getClassName() {
    return "SniiBlobStagingDirectory";
}

const char* SniiBlobStagingDirectory::getObjectName() const {
    return getClassName();
}

const std::shared_ptr<SniiBlobStagingDirectory::Buffer>* SniiBlobStagingDirectory::find_file(
        const char* name) const {
    // A null name is a caller bug, not an absent file; reporting "absent" would
    // hide it, and every caller formats `name` into its error message.
    DORIS_CHECK(name != nullptr);
    const auto it = _files.find(name);
    return it == _files.end() ? nullptr : &it->second;
}

bool SniiBlobStagingDirectory::list(std::vector<std::string>* names) const {
    DORIS_CHECK(names != nullptr);
    for (const auto& [name, buffer] : _files) {
        names->push_back(name);
    }
    return true;
}

bool SniiBlobStagingDirectory::fileExists(const char* name) const {
    return find_file(name) != nullptr;
}

int64_t SniiBlobStagingDirectory::fileModified(const char* name) const {
    // Nothing staged has a modification time, but an ABSENT name is still an
    // error -- reporting 0 for it would be the one silently-successful answer in
    // a class where every other lookup fails loudly.
    if (find_file(name) == nullptr) {
        const std::string message =
                fmt::format("File does not exist in the SNII staging directory: {}", name);
        _CLTHROWA(CL_ERR_IO, message.c_str()); // CLuceneError STRDUPs the message
    }
    return 0;
}

int64_t SniiBlobStagingDirectory::fileLength(const char* name) const {
    const std::shared_ptr<Buffer>* buffer = find_file(name);
    if (buffer == nullptr) {
        const std::string message =
                fmt::format("File does not exist in the SNII staging directory: {}", name);
        _CLTHROWA(CL_ERR_IO, message.c_str()); // CLuceneError STRDUPs the message
    }
    return static_cast<int64_t>((*buffer)->size());
}

bool SniiBlobStagingDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                         CLuceneError& err, int32_t /*bufferSize*/) {
    ret = nullptr;
    // Refused rather than implemented: a staged index is never read back from
    // here. Once it is sealed the container owns the bytes and SniiBlobDirectory
    // serves them; serving a second, pre-seal copy would be a way for a reader to
    // silently disagree with what was written.
    err.set(CL_ERR_UnsupportedOperation,
            fmt::format("SniiBlobStagingDirectory is write-only; cannot open '{}'", name).c_str());
    return false;
}

void SniiBlobStagingDirectory::renameFile(const char* /*from*/, const char* /*to*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobStagingDirectory::renameFile");
}

void SniiBlobStagingDirectory::touchFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: SniiBlobStagingDirectory::touchFile");
}

lucene::store::IndexOutput* SniiBlobStagingDirectory::createOutput(const char* name) {
    DORIS_CHECK(name != nullptr);
    // Same semantics as a filesystem directory: creating an existing name
    // truncates it. The buffer is replaced rather than cleared, so a blob source
    // already taken over the old content keeps reading the old content instead of
    // seeing it mutate underneath.
    auto buffer = std::make_shared<Buffer>();
    _files[name] = buffer;
    return _CLNEW StagingIndexOutput(std::move(buffer));
}

bool SniiBlobStagingDirectory::doDeleteFile(const char* name) {
    DORIS_CHECK(name != nullptr);
    // Reports whether anything was actually removed: Directory::deleteFile turns
    // false into an error, and claiming to have deleted a file that was never
    // staged would hide a caller's wrong name.
    return _files.erase(name) > 0;
}

void SniiBlobStagingDirectory::close() {
    // Deliberately keeps the staged files: close() is what a CLucene writer calls
    // when it is done producing, and the harvest happens afterwards. The buffers
    // die with the directory, or with the last blob source over them.
}

std::string SniiBlobStagingDirectory::toString() const {
    return fmt::format("SniiBlobStagingDirectory(files={}, bytes={})", _files.size(),
                       staged_bytes());
}

std::vector<snii::writer::BlobFileSource> SniiBlobStagingDirectory::blob_sources() const {
    std::vector<snii::writer::BlobFileSource> sources;
    sources.reserve(_files.size());
    // std::map iterates in name order, which is the order the filesystem harvest
    // produced by sorting list(). Two builds of one index therefore lay their
    // sub-files out identically in the container.
    for (const auto& [name, buffer] : _files) {
        sources.push_back(snii::writer::BlobFileSource {
                .name = name,
                .length = buffer->size(),
                .read_fn = [buffer](uint64_t offset, size_t len, uint8_t* out) -> Status {
                    if (offset > buffer->size() || len > buffer->size() - offset) {
                        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                                "SNII staging read [{}, +{}) is outside the staged {} bytes",
                                offset, len, buffer->size());
                    }
                    std::memcpy(out, buffer->data() + offset, len);
                    return Status::OK();
                }});
    }
    return sources;
}

uint64_t SniiBlobStagingDirectory::staged_bytes() const {
    uint64_t total = 0;
    for (const auto& [name, buffer] : _files) {
        total += buffer->size();
    }
    return total;
}

} // namespace doris::segment_v2::snii_doris
