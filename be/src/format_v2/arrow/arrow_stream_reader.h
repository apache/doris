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

#include <memory>
#include <optional>
#include <vector>

#include "format_v2/file_reader.h"
#include "runtime/runtime_profile.h"

namespace arrow {
class Array;
class DataType;
class MemoryPool;
class RecordBatch;
namespace ipc {
class RecordBatchStreamReader;
}
} // namespace arrow

namespace doris {
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris::format::arrow_stream {

class ArrowInputStream;

// IPC decoding binds the load planner's typed source slots, including expression inputs.
// These describe the pipe's input contract; destination casts, defaults, and row rejection
// belong to LoadTableReader.
class ArrowStreamReader final : public FileReader {
public:
    ArrowStreamReader(std::shared_ptr<io::FileSystemProperties>& properties,
                      std::unique_ptr<io::FileDescription>& file,
                      std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                      std::vector<SlotDescriptor*> file_slots, std::optional<TUniqueId> load_id);
    ~ArrowStreamReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _next_batch();
    Status _open_next_stream();
    Status _prepare_arrays();

    RuntimeState* _state = nullptr;
    const std::vector<SlotDescriptor*> _file_slots;
    const std::optional<TUniqueId> _load_id;
    std::unique_ptr<arrow::MemoryPool> _arrow_pool;
    std::vector<std::shared_ptr<arrow::DataType>> _arrow_types;
    std::vector<int> _arrow_positions;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    std::shared_ptr<ArrowInputStream> _input;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> _ipc_reader;
    std::shared_ptr<arrow::RecordBatch> _batch;
    int64_t _batch_offset = 0;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _materialize_time = nullptr;
};

} // namespace doris::format::arrow_stream
