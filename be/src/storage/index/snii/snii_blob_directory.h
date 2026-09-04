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
#include <CLucene/store/IndexInput.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/snii_doris_adapter.h"

class CLuceneError;

namespace doris::segment_v2::snii_doris {

class SniiBlobDirectory;
// lucene::store::Directory is LUCENE_REFBASE: consumers (e.g. bkd_reader)
// _CL_POINTER / _CLDECDELETE it, so the owner must release through the same
// refcount -- a plain `delete` would double-free against a holder's DEC.
using SniiBlobDirectoryPtr = std::unique_ptr<SniiBlobDirectory, DirectoryDeleter>;

// Read-only lucene::store::Directory over ONE blob logical index entry of an
// SNII container (design 2026-07-28 §5.1). It serves the entry's named files
// as BufferedIndexInputs whose readInternal lands directly in the caller's
// buffer via DorisSniiFileReader::read_into -- no per-refill allocation, no
// second GiB-scale buffer on whole-blob loads. This is the ONLY new read
// component blob indexes need: CLucene's bkd_reader and faiss's IOReader wrap
// plain Directory/IndexInput and run on it unmodified.
//
// Contract pins:
//   * openInput on a 0-length entry returns a length()==0 input (an empty BKD
//     segment stores 0-byte `bkd`/`bkd_index`; throwing would make
//     inverted_index_searcher misreport "empty" as "corrupt");
//   * close() NEVER throws (~bkd_reader is implicitly noexcept and calls it
//     when close_directory=true);
//   * write operations throw UnsupportedOperation, mirroring
//     DorisCompoundReader;
//   * clones are independent (read_into is a stateless positional read; no
//     base-stream mutex needed, unlike CSIndexInput).
class SniiBlobDirectory : public lucene::store::Directory {
public:
    // Validates the entry (blob kind; every file inside [0, data_area_end)) and
    // builds the directory. `reader` is shared into every opened input, so the
    // directory and its inputs may outlive the caller's reference.
    //
    // `data_area_end` is the container's metadata-directory offset, i.e. the
    // exclusive upper bound of the region blob bytes may occupy — obtain it from
    // SniiSegmentReader::directory_offset(). Bounding against the whole file
    // size instead would accept a corrupt entry pointing into the directory or
    // the tail.
    static Status open(std::shared_ptr<DorisSniiFileReader> reader,
                       const snii::format::LogicalIndexMetadataRef& entry, uint64_t data_area_end,
                       SniiBlobDirectoryPtr* out, int32_t read_buffer_size = -1);

    ~SniiBlobDirectory() override;

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

protected:
    bool doDeleteFile(const char* name) override;

private:
    SniiBlobDirectory(std::shared_ptr<DorisSniiFileReader> reader,
                      snii::format::LogicalIndexMetadataRef entry, int32_t read_buffer_size);

    const snii::format::NamedBlobFileRef* find_file(const char* name) const;

    std::shared_ptr<DorisSniiFileReader> _reader;
    snii::format::LogicalIndexMetadataRef _entry;
    int32_t _read_buffer_size;
    bool _closed = false;
};

} // namespace doris::segment_v2::snii_doris
