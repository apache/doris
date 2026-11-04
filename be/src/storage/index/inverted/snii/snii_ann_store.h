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
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/ann/ann_index_storage.h"
#include "storage/index/inverted/snii/format/metadata_directory.h"
#include "storage/index/inverted/snii/writer/snii_compound_writer.h"

namespace doris::snii::bkd {
class StagedBlobFile;
} // namespace doris::snii::bkd

namespace doris::segment_v2::snii_doris {

class DorisSniiFileReader;

// How SNII keeps an ANN index: as a blob logical index of kind kAnn in the segment's container,
// one named sub-file per file faiss writes (ann.faiss, and ann.ivfdata for IVF_ON_DISK), sealed in
// name order. The bytes of each sub-file are faiss's stream as it wrote it.

// The read side: the sub-files of one ANN blob, read through the container's reader, the one
// IndexFileReader opened for the segment. Holds no file of its own.
class SniiAnnSource final : public AnnIndexSource {
public:
    // `entry` is the blob's directory entry. SniiSegmentReader::open bounded its extents by the
    // container's data area, so a read inside an extent never reaches the container's metadata.
    // `read_buffer_size` is each input's buffer, config::inverted_index_read_buffer_size.
    SniiAnnSource(std::shared_ptr<DorisSniiFileReader> reader,
                  snii::format::LogicalIndexMetadataRef entry, size_t read_buffer_size);

    // Each input owns its buffer and shares the reader, whose reads are positional: inputs keep no
    // cursor in common, unlike the compound inputs of V1 and V2 over one file stream.
    Result<std::unique_ptr<AnnIndexInput>> open_input(std::string_view name) override;
    std::string debug_string() const override;

private:
    std::shared_ptr<DorisSniiFileReader> _reader;
    snii::format::LogicalIndexMetadataRef _entry;
    size_t _read_buffer_size;
};

// The write side: where an ANN index waits between its serialization and begin_close(), which
// seals it into the container.
//
// WHY IT EXISTS. A SNII container cannot take the faiss bytes as they are produced -- blob payloads
// are streamed by SniiCompoundWriter::finish(), after the text physical sections -- so the bytes
// must be parked somewhere in between. A complete in-memory copy is unbounded for HNSW and IVF
// indexes, so each file goes to the same self-cleaning staging file as native BKD, through an
// output that buffers 64 KiB at a time.
//
// THREADING. One staging belongs to one ANN column writer and is harvested later, on the close
// path; the two are sequenced, never concurrent. No locking.
class SniiAnnStaging final : public AnnIndexSink {
public:
    SniiAnnStaging() = default;
    ~SniiAnnStaging() override;

    SniiAnnStaging(const SniiAnnStaging&) = delete;
    SniiAnnStaging& operator=(const SniiAnnStaging&) = delete;

    // Creating a name again replaces its file, as a filesystem directory truncates it.
    Result<std::unique_ptr<AnnIndexOutput>> create_output(std::string_view name) override;
    std::string debug_string() const override;

    // Blob sources over the staged files, in name order, so the container lays two builds of one
    // index out identically.
    //
    // TAKES the files. This staging holds nothing afterwards and the returned sources are their
    // ONLY owners, which is what lets the container unlink each sub-file the moment it has copied
    // its bytes (see the per-blob release in SniiCompoundWriter::finish()). Leaving a second owner
    // here would defeat that: an ANN producer routinely outlives the seal -- IndexBuilder's SNII
    // ADD INDEX path keeps every AnnIndexColumnWriter alive until the whole rowset has been
    // closed -- so one rowset's staging would pile up at once.
    std::vector<snii::writer::BlobFileSource> take_blob_sources();

    // Drops every staged file NOW, whoever else still holds this staging. The abort path needs
    // that: an ANN producer keeps the staging alive through its own sink, so releasing only the
    // index file writer's reference would free nothing.
    void discard_staged_files();

    // Logical bytes across every staged file. Zero once take_blob_sources() has handed them over.
    uint64_t staged_bytes() const;

private:
    std::map<std::string, std::shared_ptr<snii::bkd::StagedBlobFile>> _files;
};

} // namespace doris::segment_v2::snii_doris
