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

Status ParquetFileHdfsScanner::open() {
    return Status();
}

Status ParquetFileHdfsScanner::get_next(vectorized::Block* block, bool* eof) {
    // todo: get block from queue
    auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    const TFileRangeDesc& range = _ranges[_next_range++];
    std::unique_ptr<FileReader> file_reader;
    RETURN_IF_ERROR(FileFactory::create_file_reader(_state->exec_env(), _profile, _params, range,
                                                    file_reader));
    _reader.reset(new ParquetReader(file_reader.release(), _file_slot_descs.size(),
                                    range.start_offset, range.size));
    Status status =
            _reader->init_reader(tuple_desc, _file_slot_descs, _conjunct_ctxs, _state->timezone());
    if (!status.ok()) {
        _scanner_eof = true;
        return Status::OK();
    }
    while (_reader->has_next()) {
        Status st = _reader->read_next_batch(block);
        if (st.is_end_of_file()) {
            break;
        }
    }
    return Status::OK();
}

void ParquetFileHdfsScanner::close() {}

void ParquetFileHdfsScanner::_prefetch_batch() {
    // 1. call file reader next batch
    // 2. push batch to queue, when get_next is called, pop batch
}

} // namespace doris::vectorized