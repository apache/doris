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
#include "exec/arrow/arrow_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "io/file_reader.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "util/string_util.h"
#include "util/thrift_util.h"
#include "vec/core/block.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris {

ArrowReaderWrap::ArrowReaderWrap(RuntimeState* state,
                                 const std::vector<SlotDescriptor*>& file_slot_descs,
                                 FileReader* file_reader, int32_t num_of_columns_from_file,
                                 bool case_sensitive)
        : _state(state),
          _file_slot_descs(file_slot_descs),
          _num_of_columns_from_file(num_of_columns_from_file),
          _case_sensitive(case_sensitive) {
    _arrow_file = std::shared_ptr<ArrowFile>(new ArrowFile(file_reader));
    _rb_reader = nullptr;
    _total_groups = 0;
    _current_group = 0;
    _statistics = std::make_shared<Statistics>();
}

ArrowReaderWrap::~ArrowReaderWrap() {
    close();
    _closed = true;
    _queue_writer_cond.notify_one();
    if (_thread.joinable()) {
        _thread.join();
    }
}

void ArrowReaderWrap::close() {
    arrow::Status st = _arrow_file->Close();
    if (!st.ok()) {
        LOG(WARNING) << "close file error: " << st.ToString();
    }
}

Status ArrowReaderWrap::column_indices() {
    _include_column_ids.clear();
    for (auto& slot_desc : _file_slot_descs) {
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        if (iter != _map_column.end()) {
            _include_column_ids.emplace_back(iter->second);
        } else {
            _missing_cols.push_back(slot_desc->col_name());
        }
    }
    return Status::OK();
}

int ArrowReaderWrap::get_column_index(std::string column_name) {
    std::string real_column_name = _case_sensitive ? column_name : to_lower(column_name);
    auto iter = _map_column.find(real_column_name);
    if (iter != _map_column.end()) {
        return iter->second;
    } else {
        std::stringstream str_error;
        str_error << "Invalid Column Name:" << real_column_name;
        LOG(WARNING) << str_error.str();
        return -1;
    }
}

Status ArrowReaderWrap::get_next_block(vectorized::Block* block, size_t* read_row, bool* eof) {
    size_t rows = 0;
    bool tmp_eof = false;
    do {
        if (_batch == nullptr || _arrow_batch_cur_idx >= _batch->num_rows()) {
            RETURN_IF_ERROR(next_batch(&_batch, &tmp_eof));
            // We need to make sure the eof is set to true iff block is empty.
            if (tmp_eof) {
                *eof = (rows == 0);
                break;
            }
        }

        size_t num_elements = std::min<size_t>((_state->batch_size() - block->rows()),
                                               (_batch->num_rows() - _arrow_batch_cur_idx));
        for (auto i = 0; i < _file_slot_descs.size(); ++i) {
            SlotDescriptor* slot_desc = _file_slot_descs[i];
            if (slot_desc == nullptr) {
                continue;
            }
            std::string real_column_name =
                    is_case_sensitive() ? slot_desc->col_name() : slot_desc->col_name_lower_case();
            auto* array = _batch->GetColumnByName(real_column_name).get();
            auto& column_with_type_and_name = block->get_by_name(slot_desc->col_name());
            RETURN_IF_ERROR(arrow_column_to_doris_column(
                    array, _arrow_batch_cur_idx, column_with_type_and_name.column,
                    column_with_type_and_name.type, num_elements, _state->timezone_obj()));
        }
        rows += num_elements;
        _arrow_batch_cur_idx += num_elements;
    } while (!tmp_eof && rows < _state->batch_size());
    *read_row = rows;
    return Status::OK();
}

Status ArrowReaderWrap::next_batch(std::shared_ptr<arrow::RecordBatch>* batch, bool* eof) {
    std::unique_lock<std::mutex> lock(_mtx);
    while (!_closed && _queue.empty()) {
        if (_batch_eof) {
            _include_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _queue_reader_cond.wait_for(lock, std::chrono::seconds(1));
    }
    if (UNLIKELY(_closed)) {
        return Status::InternalError(_status.message());
    }
    *batch = _queue.front();
    _queue.pop_front();
    _queue_writer_cond.notify_one();
    _arrow_batch_cur_idx = 0;
    return Status::OK();
}

void ArrowReaderWrap::prefetch_batch() {
    auto insert_batch = [this](const auto& batch) {
        std::unique_lock<std::mutex> lock(_mtx);
        while (!_closed && _queue.size() == _max_queue_size) {
            _queue_writer_cond.wait_for(lock, std::chrono::seconds(1));
        }
        if (UNLIKELY(_closed)) {
            return;
        }
        _queue.push_back(batch);
        _queue_reader_cond.notify_one();
    };
    int current_group = _current_group;
    int total_groups = _total_groups;
    while (true) {
        if (_closed || current_group >= total_groups) {
            _batch_eof = true;
            _queue_reader_cond.notify_one();
            return;
        }
        if (filter_row_group(current_group)) {
            current_group++;
            continue;
        }

        arrow::RecordBatchVector batches;
        read_batches(batches, current_group);
        if (!_status.ok()) {
            _closed = true;
            return;
        }
        std::for_each(batches.begin(), batches.end(), insert_batch);
        current_group++;
    }
}

ArrowFile::ArrowFile(FileReader* file) : _file(file) {}

ArrowFile::~ArrowFile() {
    arrow::Status st = Close();
    if (!st.ok()) {
        LOG(WARNING) << "close file error: " << st.ToString();
    }
}

arrow::Status ArrowFile::Close() {
    if (_file != nullptr) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
    return arrow::Status::OK();
}

bool ArrowFile::closed() const {
    if (_file != nullptr) {
        return _file->closed();
    } else {
        return true;
    }
}

arrow::Result<int64_t> ArrowFile::Read(int64_t nbytes, void* buffer) {
    return ReadAt(_pos, nbytes, buffer);
}

arrow::Result<int64_t> ArrowFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
    int64_t reads = 0;
    int64_t bytes_read = 0;
    _pos = position;
    while (nbytes > 0) {
        Status result = _file->readat(_pos, nbytes, &reads, out);
        if (!result.ok()) {
            return arrow::Status::IOError("Readat failed.");
        }
        if (reads == 0) {
            break;
        }
        bytes_read += reads; // total read bytes
        nbytes -= reads;     // remained bytes
        _pos += reads;
        out = (char*)out + reads;
    }
    return bytes_read;
}

arrow::Result<int64_t> ArrowFile::GetSize() {
    return _file->size();
}

arrow::Status ArrowFile::Seek(int64_t position) {
    _pos = position;
    // NOTE: Only readat operation is used, so _file seek is not called here.
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowFile::Tell() const {
    return _pos;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowFile::Read(int64_t nbytes) {
    auto buffer = arrow::AllocateBuffer(nbytes, arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(buffer);
    std::shared_ptr<arrow::Buffer> read_buf = std::move(buffer.ValueOrDie());
    auto bytes_read = ReadAt(_pos, nbytes, read_buf->mutable_data());
    ARROW_RETURN_NOT_OK(bytes_read);
    // If bytes_read is equal with read_buf's capacity, we just assign
    if (bytes_read.ValueOrDie() == nbytes) {
        return std::move(read_buf);
    } else {
        return arrow::SliceBuffer(read_buf, 0, bytes_read.ValueOrDie());
    }
}

} // namespace doris
