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

namespace doris::snii::bkd {
class StagedBlobFile;
}

namespace doris::segment_v2::snii_doris {

// Write-only, file-backed lucene::store::Directory: the staging area an ANN
// index is built into before it is sealed as a blob logical index of a SNII
// container.
//
// WHY IT EXISTS. A SNII container cannot take the faiss bytes as they are
// produced -- blob payloads are streamed by SniiCompoundWriter::finish(), after
// the text physical sections -- so the bytes must be parked somewhere in
// between. A complete in-memory copy is unbounded for HNSW and IVF indexes.
// Each sub-file therefore uses the same self-cleaning staging file as native
// BKD. This retains only BufferedIndexOutput's fixed buffer while still avoiding
// the throwing, whole-directory cleanup used by the V1/V2 formats.
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

    // Blob sources over the staged files, in name order -- the same order the
    // filesystem harvest produced by sorting list(), so the container lays two
    // builds of one index out identically.
    //
    // TAKES the files. This directory holds nothing afterwards and the returned
    // sources are their ONLY owners, which is what lets the container unlink each
    // sub-file the moment it has copied its bytes (see the per-blob release in
    // SniiCompoundWriter::finish()). Leaving a second owner here would defeat
    // that: an ANN producer routinely outlives the seal -- IndexBuilder's SNII
    // ADD INDEX path keeps every AnnIndexColumnWriter alive until the whole
    // rowset has been closed -- so one rowset's staging would pile up at once.
    std::vector<snii::writer::BlobFileSource> take_blob_sources();

    // Drops every staged file NOW, whoever else still holds this directory. The
    // abort path needs that: an ANN producer keeps the directory alive through
    // its own _dir, so releasing only the index file writer's reference would
    // free nothing.
    void discard_staged_files();

    // Logical bytes across every staged sub-file. Zero once take_blob_sources()
    // has handed them over.
    uint64_t staged_bytes() const;

protected:
    bool doDeleteFile(const char* name) override;

private:
    class StagingIndexOutput;

    const std::shared_ptr<snii::bkd::StagedBlobFile>* find_file(const char* name) const;

    std::map<std::string, std::shared_ptr<snii::bkd::StagedBlobFile>> _files;
};

} // namespace doris::segment_v2::snii_doris
