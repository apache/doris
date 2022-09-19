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

#include "exec/parquet_scanner.h"

#include "exec/arrow/parquet_reader.h"
#include "io/file_factory.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_pipe.h"

namespace doris {

ParquetScanner::ParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                               const TBrokerScanRangeParams& params,
                               const std::vector<TBrokerRangeDesc>& ranges,
                               const std::vector<TNetworkAddress>& broker_addresses,
                               const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          // _splittable(params.splittable),
          _cur_file_reader(nullptr),
          _cur_file_eof(false) {}

ParquetScanner::~ParquetScanner() {
    close();
}

Status ParquetScanner::open() {
    return BaseScanner::open();
}

Status ParquetScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) {
    SCOPED_TIMER(_read_timer);
    // Get one line
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_file_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                continue;
            }
            _cur_file_eof = false;
        }
        RETURN_IF_ERROR(_cur_file_reader->read(_src_tuple, tuple_pool, &_cur_file_eof));
        // range of current file
        const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_slots_of_columns_from_path(range.num_of_columns_from_file,
                                            range.columns_from_path);
        }

        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        RETURN_IF_ERROR(fill_dest_tuple(tuple, tuple_pool, fill_tuple));
        break; // break always
    }

    *eof = _scanner_eof;
    return Status::OK();
}

Status ParquetScanner::open_next_reader() {
    // open_file_reader
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }

    while (true) {
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TBrokerRangeDesc& range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                range.file_type, _state->exec_env(), _profile, _broker_addresses,
                _params.properties, range, range.start_offset, file_reader));
        RETURN_IF_ERROR(file_reader->open());

        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }
        int32_t num_of_columns_from_file = _src_slot_descs.size();
        if (range.__isset.num_of_columns_from_file) {
            num_of_columns_from_file = range.num_of_columns_from_file;
        }
        _cur_file_reader = new ParquetReaderWrap(_state, _src_slot_descs, file_reader.release(),
                                                 num_of_columns_from_file, 0, 0);
        auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
        Status status =
                _cur_file_reader->init_reader(tuple_desc, _conjunct_ctxs, _state->timezone());
        if (status.is_end_of_file()) {
            continue;
        } else {
            if (!status.ok()) {
                return Status::InternalError("file: {}, error:{}", range.path,
                                             status.get_error_msg());
            } else {
                RETURN_IF_ERROR(_cur_file_reader->init_parquet_type());
                return status;
            }
        }
    }
}

void ParquetScanner::close() {
    BaseScanner::close();
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }
}

} // namespace doris
