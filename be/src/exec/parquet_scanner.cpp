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

#include "exec/broker_reader.h"
#include "exec/buffered_reader.h"
#include "exec/decompressor.h"
#include "exec/local_file_reader.h"
#include "exec/parquet_reader.h"
#include "exec/s3_reader.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/raw_value.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "exec/parquet_reader.h"
#include "exprs/expr.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"
#include "exec/buffered_reader.h"
#include "exec/decompressor.h"
#include "exec/parquet_reader.h"

#if defined(__x86_64__)
    #include "exec/hdfs_file_reader.h"
#endif

namespace doris {

ParquetScanner::ParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                               const TBrokerScanRangeParams& params,
                               const std::vector<TBrokerRangeDesc>& ranges,
                               const std::vector<TNetworkAddress>& broker_addresses,
                               const std::vector<ExprContext*>& pre_filter_ctxs,
                               ScannerCounter* counter)
        : BaseScanner(state, profile, params, pre_filter_ctxs, counter),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          // _splittable(params.splittable),
          _cur_file_reader(nullptr),
          _next_range(0),
          _cur_file_eof(false),
          _scanner_eof(false) {}

ParquetScanner::~ParquetScanner() {
    close();
}

Status ParquetScanner::open() {
    return BaseScanner::open();
}

Status ParquetScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
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
        RETURN_IF_ERROR(
                _cur_file_reader->read(_src_tuple, _src_slot_descs, tuple_pool, &_cur_file_eof));
        // range of current file
        const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_slots_of_columns_from_path(range.num_of_columns_from_file,
                                            range.columns_from_path);
        }

        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        if (fill_dest_tuple(tuple, tuple_pool)) {
            break; // break if true
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
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
        switch (range.file_type) {
        case TFileType::FILE_LOCAL: {
            file_reader.reset(new LocalFileReader(range.path, range.start_offset));
            break;
        }
        case TFileType::FILE_HDFS: {
#if defined(__x86_64__)
            file_reader.reset(new HdfsFileReader(
                    range.hdfs_params, range.path, range.start_offset));
            break;
#else
            return Status::InternalError("HdfsFileReader do not support on non x86 platform");
#endif
        }
        case TFileType::FILE_BROKER: {
            int64_t file_size = 0;
            // for compatibility
            if (range.__isset.file_size) {
                file_size = range.file_size;
            }
            file_reader.reset(new BufferedReader(_profile,
                    new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
                                     range.path, range.start_offset, file_size)));
            break;
        }
        case TFileType::FILE_S3: {
            file_reader.reset(new BufferedReader(_profile,
                    new S3Reader(_params.properties, range.path, range.start_offset)));
            break;
        }
#if 0
            case TFileType::FILE_STREAM:
        {
            _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
            if (_stream_load_pipe == nullptr) {
                return Status::InternalError("unknown stream load id");
            }
            _cur_file_reader = _stream_load_pipe.get();
            break;
        }
#endif
        default: {
            std::stringstream ss;
            ss << "Unknown file type, type=" << range.file_type;
            return Status::InternalError(ss.str());
        }
        }
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }
        if (range.__isset.num_of_columns_from_file) {
            _cur_file_reader =
                    new ParquetReaderWrap(file_reader.release(), range.num_of_columns_from_file);
        } else {
            _cur_file_reader = new ParquetReaderWrap(file_reader.release(), _src_slot_descs.size());
        }

        Status status = _cur_file_reader->init_parquet_reader(_src_slot_descs, _state->timezone());

        if (status.is_end_of_file()) {
            continue;
        } else {
            if (!status.ok()) {
                std::stringstream ss;
                ss << " file: " << range.path << " error:" << status.get_error_msg();
                return Status::InternalError(ss.str());
            } else {
                return status;
            }
        }
    }
}

void ParquetScanner::close() {
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
