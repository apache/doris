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

#include "arrow_stream_reader.h"

#include "arrow/array.h"
#include "arrow/io/buffered.h"
#include "arrow/io/stdio.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow_pip_input_stream.h"
#include "common/logging.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/wal_manager.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris {
class RuntimeProfile;
} // namespace doris

namespace doris::vectorized {

ArrowStreamReader::ArrowStreamReader(RuntimeState* state, RuntimeProfile* profile,
                                     ScannerCounter* counter, const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range,
                                     const std::vector<SlotDescriptor*>& file_slot_descs,
                                     io::IOContext* io_ctx)
        : _state(state), _range(range), _file_slot_descs(file_slot_descs), _file_reader(nullptr) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
}

ArrowStreamReader::~ArrowStreamReader() = default;

Status ArrowStreamReader::init_reader() {
    RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state, false));
    _pip_stream = ArrowPipInputStream::create_unique(_file_reader);
    return Status::OK();
}

Status ArrowStreamReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    bool has_next = false;
    RETURN_IF_ERROR(_pip_stream->HasNext(&has_next));
    if (!has_next) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    // create a reader to read data
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> tRet =
            arrow::ipc::RecordBatchStreamReader::Open(_pip_stream.get(),
                                                      arrow::ipc::IpcReadOptions::Defaults());
    if (!tRet.ok()) {
        LOG(WARNING) << "failed to open stream reader: " << tRet.status().message();
        return Status::InternalError("failed to open stream reader: {}", tRet.status().message());
    }
    auto reader = std::move(tRet).ValueUnsafe();

    // get arrow data from reader
    arrow::Result<arrow::RecordBatchVector> tRet2 = reader->ToRecordBatches();
    if (!tRet2.ok()) {
        LOG(WARNING) << "failed to read batch: " << tRet2.status().message();
        return Status::InternalError("failed to read batch: {}", tRet2.status().message());
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches = std::move(tRet2).ValueUnsafe();

    // convert arrow batch to block
    auto columns = block->mutate_columns();
    int batch_size = out_batches.size();
    for (int i = 0; i < batch_size; i++) {
        arrow::RecordBatch& batch = *out_batches[i];
        int num_rows = batch.num_rows();
        int num_columns = batch.num_columns();
        for (int c = 0; c < num_columns; ++c) {
            arrow::Array* column = batch.column(c).get();

            std::string column_name = batch.schema()->field(c)->name();

            vectorized::ColumnWithTypeAndName& column_with_name = block->get_by_name(column_name);

            column_with_name.type->get_serde()->read_column_from_arrow(
                    column_with_name.column->assume_mutable_ref(), column, 0, num_rows, _ctzz);
        }
        *read_rows += batch.num_rows();
    }

    *eof = (*read_rows == 0);
    return Status::OK();
}

Status ArrowStreamReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                      std::unordered_set<std::string>* missing_cols) {
    for (auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

} // namespace doris::vectorized