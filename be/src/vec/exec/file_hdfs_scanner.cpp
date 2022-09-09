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

#include "file_hdfs_scanner.h"

#include "io/file_factory.h"

namespace doris::vectorized {

ParquetFileHdfsScanner::ParquetFileHdfsScanner(RuntimeState* state, RuntimeProfile* profile,
                                               const TFileScanRangeParams& params,
                                               const std::vector<TFileRangeDesc>& ranges,
                                               const std::vector<TExpr>& pre_filter_texprs,
                                               ScannerCounter* counter)
        : HdfsFileScanner(state, profile, params, ranges, pre_filter_texprs, counter) {}

ParquetFileHdfsScanner::~ParquetFileHdfsScanner() {
    ParquetFileHdfsScanner::close();
}

Status ParquetFileHdfsScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_ranges.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_get_next_reader());
    return Status();
}

void ParquetFileHdfsScanner::_init_profiles(RuntimeProfile* profile) {}

Status ParquetFileHdfsScanner::get_next(vectorized::Block* block, bool* eof) {
    if (_scanner_eof) {
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(init_block(block));
    bool range_eof = false;
    RETURN_IF_ERROR(_reader->read_next_batch(block, &range_eof));
    if (block->rows() > 0) {
        _fill_columns_from_path(block, block->rows());
    }
    if (range_eof) {
        RETURN_IF_ERROR(_get_next_reader());
        *eof = _scanner_eof;
    }
    return Status::OK();
}

Status ParquetFileHdfsScanner::_get_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    const TFileRangeDesc& range = _ranges[_next_range++];
    std::unique_ptr<FileReader> file_reader;
    RETURN_IF_ERROR(FileFactory::create_file_reader(_state->exec_env(), _profile, _params, range,
                                                    file_reader));
    _reader.reset(new ParquetReader(
            file_reader.release(), _file_slot_descs.size(), _state->query_options().batch_size,
            range.start_offset, range.size, const_cast<cctz::time_zone*>(&_state->timezone_obj())));
    auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
    Status status =
            _reader->init_reader(tuple_desc, _file_slot_descs, _conjunct_ctxs, _state->timezone());
    if (!status.ok()) {
        if (status.is_end_of_file()) {
            return _get_next_reader();
        }
        return status;
    }
    return Status::OK();
}

void ParquetFileHdfsScanner::close() {
    FileScanner::close();
}

} // namespace doris::vectorized