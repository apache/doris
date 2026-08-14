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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexOutput.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "storage/index/snii/writer/snii_compound_writer.h"

class CLuceneError;

namespace doris::segment_v2::snii_doris {

// Write-only, memory-backed lucene::store::Directory: the staging area an ANN
// index is built into before it is sealed as a blob logical index of a SNII
// container.
//
// WHY IT EXISTS. A SNII container cannot take the faiss bytes as they are
// produced -- blob payloads are streamed by SniiCompoundWriter::finish(), after
// the text physical sections -- so the bytes must be parked somewhere in
// between. Borrowing a CLucene filesystem directory for that, as the V1/V2
// formats do, buys two problems SNII has no use for: the temp directory is
// removed by exactly one call, so every early return on the close path leaks it
// until a BE restart wipes the tmp dir; and that call, deleteDirectory(),
// throws CLuceneError, which must not cross a Status-returning close. Parking
// the bytes here removes both -- nothing lands on disk, and there is nothing to
// delete.
//
// SCOPE. Only the write side is real. The faiss writer uses createOutput() and
// toString() and nothing else, and a sealed ANN blob is read back through
// SniiBlobDirectory over the container, never through here -- so openInput()
// refuses rather than pretending to serve a staged file.
//
// THREADING. One directory belongs to one ANN column writer and is harvested
// later, on the close path; the two are sequenced, never concurrent. No locking.
class SniiBlobStagingDirectory : public lucene::store::Directory {
public:
    SniiBlobStagingDirectory() = default;
    ~SniiBlobStagingDirectory() override;

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    static const char* getClassName();
    const char* getObjectName() const override;

    // Blob sources over the staged buffers, in name order -- the same order the
    // filesystem harvest produced by sorting list(), so the container lays two
    // builds of one index out identically.
    //
    // Each source keeps its buffer alive on its own, so the sources stay valid
    // after this directory is destroyed: SniiCompoundWriter::finish() pulls them
    // long after the ANN writer is gone.
    std::vector<snii::writer::BlobFileSource> blob_sources() const;

    // Bytes currently held in memory across every staged sub-file.
    uint64_t staged_bytes() const;

protected:
    bool doDeleteFile(const char* name) override;

private:
    class StagingIndexOutput;

    using Buffer = std::vector<uint8_t>;

    const std::shared_ptr<Buffer>* find_file(const char* name) const;

    std::map<std::string, std::shared_ptr<Buffer>> _files;
};

} // namespace doris::segment_v2::snii_doris
