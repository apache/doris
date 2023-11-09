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

#include "arrow_reader.h"

#include "common/logging.h"
#include "olap/wal_manager.h"
#include "runtime/runtime_state.h"
#include "arrow/array.h"
#include "arrow/ipc/reader.h"
#include "arrow/io/buffered.h"
#include "arrow/io/stdio.h"
#include "arrow/result.h"
#include "arrow/ipc/options.h"
#include "arrow/record_batch.h"
#include "io/fs/stream_load_pipe.h"

namespace doris {
class RuntimeProfile;
} // namespace vectorized

namespace doris::vectorized {

ArrowReader::ArrowReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                     const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx)
                    //  {
        : _state(state),
        //   _profile(profile),
        //   _counter(counter),
        //   _params(params),
          _range(range),
          _file_reader(nullptr) {

}

ArrowReader::~ArrowReader() = default;

Status ArrowReader::init_reader() {
    RETURN_IF_ERROR(
        FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state, false));
    return Status::OK();
}
Status ArrowReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {

    size_t read_size = 0;
    RETURN_IF_ERROR((dynamic_cast<io::StreamLoadPipe*>(_file_reader.get()))
                        ->read_one_message(&_file_buf, &read_size));


    auto buf_reader = std::make_shared<arrow::io::BufferReader>(_file_buf.get(), (int64_t)read_size);
    // ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(buf_reader, arrow::ipc::IpcReadOptions::Defaults()));
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;
    // ARROW_ASSIGN_OR_RAISE(out_batches, reader->ToRecordBatches());

    // TODO use ARROW_ASSIGN_OR_RAISE
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> tmp = arrow::ipc::RecordBatchStreamReader::Open(buf_reader, arrow::ipc::IpcReadOptions::Defaults());
    if (!tmp.ok()) {
        LOG(ERROR) << "open stream reader failed";
    }
    auto reader = std::move(tmp).ValueUnsafe();

    arrow::Result<arrow::RecordBatchVector> tmp2 = reader->ToRecordBatches();
    if (!tmp2.ok()) {
        LOG(ERROR) << "open stream reader failed";
    }
    out_batches = std::move(tmp2).ValueUnsafe();

    // TODO RecordBatch -> block
    // int batch_size = out_batches.size();
    // for (int i=0; i<batch_size; i++)
    // {
    //     arrow::RecordBatch& batch = *out_batches[i];
    //     // 列个数
    //     int num_rows = batch.num_rows();
    //     int num_columns = batch.num_columns();
    //     // LOG(INFO) << "wuwenchi xxxx batch.num_rows:" << num_rows;
    //     // LOG(INFO) << "wuwenchi xxxx batch.num_columns:" << num_columns;
    //     for (int c = 0; c < num_columns; ++c) {
    //         arrow::Array* column = batch.column(c).get();
            
    //         auto pt = doris::vectorized::arrow_type_to_primitive_type(column->type_id());
    //         vectorized::DataTypePtr data_type = vectorized::DataTypeFactory::instance().create_data_type(pt, true);
    //         vectorized::MutableColumnPtr data_column = data_type->create_column();
    //         vectorized::ColumnWithTypeAndName column_with_name(std::move(data_column), data_type, "test_numeric_column");

    //         Status st = doris::vectorized::arrow_column_to_doris_column(column, 0, column_with_name.column, column_with_name.type, num_rows, ctzz);

    //         // for (int k=0; k<num_rows; k++) {
    //         //     LOG(INFO) << "wuwenchi xxxx column_with_name.to_string : " << k << ": " << column_with_name.to_string(k);
    //         // }
    //     }
    // }

    *read_rows = 0;
    *eof = true;
    return Status::OK();
}

Status ArrowReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    return Status::OK();
}

} // namespace doris::vectorized