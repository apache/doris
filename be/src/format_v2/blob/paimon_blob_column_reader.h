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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/column/column_vector.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace io {
struct FileSystemProperties;
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format {
class BlobFileReader;
} // namespace doris::format

namespace doris::format::paimon {

// Reads one top-level BLOB column using Paimon Default blob storage. Managed .blob files
// are joined by logical row ID; descriptor-only/external-storage layouts are handled by JNI.
// Output may contain inline bytes or a descriptor, independently of this storage restriction.
// This reader has no knowledge of output Block positions.
class PaimonBlobColumnReader {
public:
    // The context is borrowed for read_blob(). Scanner-owned profile and I/O statistics
    // must outlive this reader, since the opened file readers retain them.
    struct ReadContext {
        const io::FileSystemProperties& system_properties;
        RuntimeState* runtime_state;
        const io::IOContext* io_ctx;
        RuntimeProfile* profile;
        const std::string& fs_name;
        bool file_cache_admission;
        // Per-value inline payload limit; descriptor output does not read payload bytes.
        int64_t blob_max_size;
    };

    // Sort a nonempty group for one field and verify contiguous coverage of the whole anchor.
    // The caller validates field IDs and the non-overflowing anchor range first. This is also
    // used for unprojected groups, so corrupt metadata cannot be mistaken for a missing column.
    static Status validate_blob_files(std::vector<const TPaimonBlobFileDesc*>* files,
                                      int64_t anchor_first_row_id, int64_t anchor_row_count);

    // Copies descriptors from an already validated/sorted group; borrows no metadata pointers.
    // An empty group means this field was absent when the anchor was written.
    PaimonBlobColumnReader(const ColumnDefinition& column,
                           const std::vector<const TPaimonBlobFileDesc*>& files,
                           bool blob_as_descriptor);

    ~PaimonBlobColumnReader();

    // anchor_row_positions are zero-based original positions in the complete Parquet file,
    // preserved across filtering and byte splits. Returns one FILE value per input position;
    // files and decoded indexes are opened lazily.
    Status read_blob(const ColumnInt64& anchor_row_positions, int64_t anchor_first_row_id,
                     const ReadContext& context, MutableColumnPtr* blob_column_result);

private:
    struct BlobFileState {
        TPaimonBlobFileDesc blob_metadata;
        // Created on first access; metadata and decoded indexes are reused across batches.
        std::unique_ptr<BlobFileReader> blob_reader;
    };

    Status _open_blob_file(BlobFileState* blob_file, const ReadContext& context);

    std::string _blob_column_name;
    DataTypePtr _blob_column_type;
    std::vector<BlobFileState> _blob_files;
    bool _blob_as_descriptor;
};

} // namespace doris::format::paimon
