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

#include "vec/exec/vparquet_scanner.h"

#include "exec/arrow/parquet_reader.h"

namespace doris::vectorized {

VParquetScanner::VParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                                 const TBrokerScanRangeParams& params,
                                 const std::vector<TBrokerRangeDesc>& ranges,
                                 const std::vector<TNetworkAddress>& broker_addresses,
                                 const std::vector<TExpr>& pre_filter_texprs,
                                 ScannerCounter* counter)
        : VArrowScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs,
                        counter) {}

ArrowReaderWrap* VParquetScanner::_new_arrow_reader(
        const std::vector<SlotDescriptor*>& file_slot_descs, FileReader* file_reader,
        int32_t num_of_columns_from_file, int64_t range_start_offset, int64_t range_size) {
    return new ParquetReaderWrap(_state, file_slot_descs, file_reader, num_of_columns_from_file,
                                 range_start_offset, range_size);
}

} // namespace doris::vectorized
