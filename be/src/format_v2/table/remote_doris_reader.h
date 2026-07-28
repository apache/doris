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

namespace doris::format::remote_doris {

// Small abstraction around Arrow Flight to keep Remote Doris v2 reader unit-testable without
// starting a Flight server. Production code uses FlightRemoteDorisStream; tests can provide
// RecordBatch-backed streams that exercise the same FileReader block materialization path.
class RemoteDorisStream {
public:
    virtual ~RemoteDorisStream() = default;
    virtual Status next(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
    virtual Status close() = 0;
};

using RemoteDorisStreamFactory =
        std::function<Status(const TFileRangeDesc&, std::unique_ptr<RemoteDorisStream>*)>;

class RemoteDorisFileReader final : public FileReader {
public:
    RemoteDorisFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                          std::unique_ptr<io::FileDescription>& file_description,
                          std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                          const TFileRangeDesc& range,
                          const std::vector<SlotDescriptor*>& file_slot_descs,
                          RemoteDorisStreamFactory stream_factory = {});
    ~RemoteDorisFileReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _open_stream();
    Status _materialize_record_batch(const arrow::RecordBatch& batch, Block* file_block,
                                     size_t* rows) const;
    Status _materialize_arrow_column(const arrow::RecordBatch& batch, int arrow_column_idx,
                                     LocalColumnId file_column_id, const LocalIndex& block_position,
                                     Block* file_block) const;
    Status _build_col_name_to_file_id();

    const TFileRangeDesc _range;
    const std::vector<SlotDescriptor*> _file_slot_descs;
    RemoteDorisStreamFactory _stream_factory;
    cctz::time_zone _ctz;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _open_stream_time = nullptr;
    RuntimeProfile::Counter* _next_batch_time = nullptr;
    RuntimeProfile::Counter* _io_time = nullptr;
    RuntimeProfile::Counter* _materialize_time = nullptr;
    RuntimeProfile::Counter* _filter_time = nullptr;
    RuntimeState* _runtime_state = nullptr;
    int _flight_timeout_seconds = 300;
    std::unique_ptr<RemoteDorisStream> _stream;
    std::unordered_map<std::string, LocalColumnId> _col_name_to_file_id;
};

class RemoteDorisReader final : public TableReader {
public:
    explicit RemoteDorisReader(RemoteDorisStreamFactory stream_factory = {});

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;

private:
    RemoteDorisStreamFactory _stream_factory;
};

} // namespace doris::format::remote_doris
