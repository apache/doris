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

#include <utility>

#include "common/check.h"
#include "common/status.h"
#include "storage/index/snii/bkd/staged_blob_file.h"

namespace doris::segment_v2::snii_doris {

// Appends into one staged file. BufferedIndexOutput batches the writer's
// byte-at-a-time calls, so flushBuffer issues 64 KiB sequential writes while the
// complete ANN payload remains outside the process heap.
class SniiBlobStagingDirectory::StagingIndexOutput final
        : public lucene::store::BufferedIndexOutput {
public:
    explicit StagingIndexOutput(std::shared_ptr<snii::bkd::StagedBlobFile> file)
            : _file(std::move(file)) {}

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
            // A destructor may not throw. The normal success path closes
            // explicitly so it can report this staging I/O failure.
        }
    }

    void close() override {
        if (_closed) {
            return;
        }
        BufferedIndexOutput::close();
        Status status = _file->finalize();
        if (!status.ok()) {
            const std::string message = status.to_string();
            _CLTHROWA(CL_ERR_IO, message.c_str());
        }
        _closed = true;
    }

    int64_t length() const override { return static_cast<int64_t>(_file->bytes_written()); }

protected:
    void flushBuffer(const uint8_t* b, const int32_t size) override {
        // flush() is also called with an empty buffer (on close, and on every
        // seek), which is not an error.
        if (b == nullptr || size <= 0) {
            return;
        }
        Status status = _file->append(snii::Slice(b, static_cast<size_t>(size)));
        if (!status.ok()) {
            const std::string message = status.to_string();
            _CLTHROWA(CL_ERR_IO, message.c_str());
        }
    }

private:
    const std::shared_ptr<snii::bkd::StagedBlobFile> _file;
    bool _closed = false;
};

SniiBlobStagingDirectory::~SniiBlobStagingDirectory() = default;

const char* SniiBlobStagingDirectory::getClassName() {
    return "SniiBlobStagingDirectory";
}

const char* SniiBlobStagingDirectory::getObjectName() const {
    return getClassName();
}

const std::shared_ptr<snii::bkd::StagedBlobFile>* SniiBlobStagingDirectory::find_file(
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
    const std::shared_ptr<snii::bkd::StagedBlobFile>* file = find_file(name);
    if (file == nullptr) {
        const std::string message =
                fmt::format("File does not exist in the SNII staging directory: {}", name);
        _CLTHROWA(CL_ERR_IO, message.c_str()); // CLuceneError STRDUPs the message
    }
    return static_cast<int64_t>((*file)->bytes_written());
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
    // truncates it. The file is replaced rather than reused, so a blob source
    // already taken over the old content keeps reading the old content instead
    // of seeing it mutate underneath.
    std::unique_ptr<snii::bkd::StagedBlobFile> created;
    Status status = snii::bkd::StagedBlobFile::create(name, &created);
    if (!status.ok()) {
        const std::string message = status.to_string();
        _CLTHROWA(CL_ERR_IO, message.c_str());
    }
    auto file = std::shared_ptr<snii::bkd::StagedBlobFile>(std::move(created));
    _files[name] = file;
    return _CLNEW StagingIndexOutput(std::move(file));
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
    // when it is done producing, and the harvest happens afterwards. Each temp
    // file is unlinked by its final StagedBlobFile owner.
}

std::string SniiBlobStagingDirectory::toString() const {
    return fmt::format("SniiBlobStagingDirectory(files={}, bytes={})", _files.size(),
                       staged_bytes());
}

std::vector<snii::writer::BlobFileSource> SniiBlobStagingDirectory::take_blob_sources() {
    // Moved out before anything is built from it: a source has to be the file's
    // only owner, or the per-blob release in SniiCompoundWriter::finish() frees
    // nothing and the staging survives until this directory does.
    std::map<std::string, std::shared_ptr<snii::bkd::StagedBlobFile>> taken;
    taken.swap(_files);
    std::vector<snii::writer::BlobFileSource> sources;
    sources.reserve(taken.size());
    // std::map iterates in name order, which is the order the filesystem harvest
    // produced by sorting list(). Two builds of one index therefore lay their
    // sub-files out identically in the container.
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

uint64_t SniiBlobStagingDirectory::staged_bytes() const {
    uint64_t total = 0;
    for (const auto& [name, file] : _files) {
        total += file->bytes_written();
    }
    return total;
}

} // namespace doris::segment_v2::snii_doris
