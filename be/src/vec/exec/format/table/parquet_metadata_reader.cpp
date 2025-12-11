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

#include "vec/exec/format/table/parquet_metadata_reader.h"

#include "vec/core/block.h"

namespace doris::vectorized {

ParquetMetadataReader::ParquetMetadataReader(const std::vector<SlotDescriptor*>& slots,
                                             RuntimeState* state, RuntimeProfile* profile,
                                             const TMetaScanRange& scan_range)
        : _state(state), _profile(profile), _scan_range(scan_range) {
    (void)slots;
}

Status ParquetMetadataReader::init_reader() {
    RETURN_IF_ERROR(_init_from_scan_range(_scan_range));
    return Status::OK();
}

Status ParquetMetadataReader::_init_from_scan_range(const TMetaScanRange& scan_range) {
    if (!scan_range.__isset.parquet_params) {
        return Status::InvalidArgument(
                "Missing parquet parameters for parquet_metadata table function");
    }
    const TParquetMetadataParams& params = scan_range.parquet_params;
    if (params.__isset.paths) {
        _paths.assign(params.paths.begin(), params.paths.end());
    }
    if (params.__isset.mode) {
        _mode = params.mode;
    } else {
        _mode = "parquet_metadata";
    }
    return Status::OK();
}

Status ParquetMetadataReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    (void)block;
    if (_eof) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }

    _eof = true;
    *eof = true;
    *read_rows = 0;
    return Status::OK();
}

Status ParquetMetadataReader::close() {
    return Status::OK();
}
} // namespace doris::vectorized
