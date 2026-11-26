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

#include <arrow/c/abi.h>
#include <arrow/record_batch.h>
#include <cctz/time_zone.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "format_v2/file_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris::format::adbc {

// Replaces *stream with one that delegates to it and, on release, clears its own release callback
// the way the Arrow C data interface requires. *stream is left released, as a move leaves it.
//
// Not a nicety: Arrow C++ aborts the process (ArrowArrayStreamRelease's assertion) when a release
// callback returns without clearing itself, and the Flight SQL driver's stream does exactly that --
// observed, and the reason a scan used to take the BE down. The ADBC driver manager's own wrapper
// hides the flaw, but this connector loads drivers through the registry that owns their lifetime and
// so calls their entry points directly. Applied to every driver on purpose: any of them may have the
// same flaw, and a third-party driver's bug must not be able to abort the BE.
void enforce_stream_release_contract(ArrowArrayStream* stream);

// Small abstraction around the ADBC C API so the block materialization path stays unit-testable
// without a live database. Production uses the real ADBC stream; tests can drive the same path from
// plain RecordBatches. Mirrors RemoteDorisStream deliberately -- the two readers differ only in
// where the Arrow stream comes from.
class AdbcStream {
public:
    virtual ~AdbcStream() = default;
    // Sets *batch to nullptr at end of stream.
    virtual Status next(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
    virtual Status close() = 0;
};

using AdbcStreamFactory =
        std::function<Status(const TFileRangeDesc&, std::unique_ptr<AdbcStream>*)>;

class AdbcFileReader final : public FileReader {
public:
    AdbcFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                   std::unique_ptr<io::FileDescription>& file_description,
                   std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                   const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& file_slot_descs,
                   AdbcStreamFactory stream_factory = {});
    ~AdbcFileReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _open_stream();
    Status _materialize_record_batch(const arrow::RecordBatch& batch, Block* file_block,
                                     size_t* rows) const;
    // Takes the already-normalized array rather than the batch column: third-party drivers emit
    // Arrow variants the serdes reject, so normalization has to happen before this point.
    Status _materialize_arrow_column(const std::string& column_name,
                                     const std::shared_ptr<arrow::Array>& array, int64_t num_rows,
                                     LocalColumnId file_column_id, const LocalIndex& block_position,
                                     Block* file_block) const;
    Status _build_col_name_to_file_id();

    const TFileRangeDesc _range;
    const std::vector<SlotDescriptor*> _file_slot_descs;
    AdbcStreamFactory _stream_factory;
    cctz::time_zone _ctz;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _open_stream_time = nullptr;
    RuntimeProfile::Counter* _next_batch_time = nullptr;
    RuntimeProfile::Counter* _io_time = nullptr;
    RuntimeProfile::Counter* _normalize_time = nullptr;
    RuntimeProfile::Counter* _materialize_time = nullptr;
    RuntimeProfile::Counter* _filter_time = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::unique_ptr<AdbcStream> _stream;
    std::unordered_map<std::string, LocalColumnId> _col_name_to_file_id;
};

class AdbcReader final : public TableReader {
public:
    explicit AdbcReader(AdbcStreamFactory stream_factory = {});

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;

private:
    AdbcStreamFactory _stream_factory;
};

} // namespace doris::format::adbc
