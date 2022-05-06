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

#ifndef ORC_SCANNER_H
#define ORC_SCANNER_H

#include <orc/OrcFile.hh>

#include "exec/base_scanner.h"

namespace doris {

// Broker scanner convert the data read from broker to doris's tuple.
class ORCScanner : public BaseScanner {
public:
    ORCScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
               const std::vector<TBrokerRangeDesc>& ranges,
               const std::vector<TNetworkAddress>& broker_addresses,
               const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~ORCScanner() override;

    // Open this scanner, will initialize information need to
    Status open() override;

    // Get next tuple
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) override;

    // Close this scanner
    void close() override;

private:
    // Read next buffer from reader
    Status open_next_reader();

private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    // Reader
    int _next_range;
    bool _cur_file_eof;

    // orc file reader object
    orc::ReaderOptions _options;
    orc::RowReaderOptions _row_reader_options;
    std::shared_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    // The batch after reading from orc data is arranged in the original order,
    // so we need to record the index in the original order to correspond the column names to the order
    std::vector<int> _position_in_orc_original;
    int _num_of_columns_from_file;

    int _total_groups; // groups in a orc file
    int _current_group;
    int64_t _rows_of_group; // rows in a group.
    int64_t _current_line_of_group;
};

} // namespace doris
#endif //ORC_SCANNER_H
