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

#pragma once

#include <exec/broker_scanner.h>

namespace doris::vectorized {
class VBrokerScanner final : public BrokerScanner {
public:
    VBrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                   const TBrokerScanRangeParams& params,
                   const std::vector<TBrokerRangeDesc>& ranges,
                   const std::vector<TNetworkAddress>& broker_addresses,
                   const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);
    ~VBrokerScanner() override = default;

    Status get_next(std::vector<MutableColumnPtr>& columns, bool* eof) override;

private:
    Status _convert_one_row(const Slice& line, std::vector<MutableColumnPtr>& columns);
    Status _fill_dest_columns(std::vector<MutableColumnPtr>& columns);
};
} // namespace doris::vectorized