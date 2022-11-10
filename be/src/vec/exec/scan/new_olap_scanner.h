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

#include "exec/olap_utils.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "exprs/hybrid_set.h"
#include "olap/reader.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {

struct OlapScanRange;

namespace vectorized {

class NewOlapScanNode;

class NewOlapScanner : public VScanner {
public:
    NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit, bool aggregation,
                   bool need_agg_finalize, const TPaloScanRange& scan_range,
                   RuntimeProfile* profile);

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

public:
    Status prepare(const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   VExprContext** vconjunct_ctx_ptr, const std::vector<TCondition>& filters,
                   const std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>>&
                           bloom_filters,
                   const std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>>& in_filters,
                   const std::vector<FunctionFilter>& function_filters);

    const std::string& scan_disk() const { return _tablet->data_dir()->path(); }

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _update_counters_before_close() override;

private:
    void _update_realtime_counters();

    Status _init_tablet_reader_params(
            const std::vector<OlapScanRange*>& key_ranges, const std::vector<TCondition>& filters,
            const std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>>&
                    bloom_filters,
            const std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>>& in_filters,
            const std::vector<FunctionFilter>& function_filters);

    Status _init_return_columns();

private:
    bool _aggregation;
    bool _need_agg_finalize;

    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;
    int64_t _version;

    TabletReader::ReaderParams _tablet_reader_params;
    std::unique_ptr<TabletReader> _tablet_reader;

    std::vector<uint32_t> _return_columns;
    std::unordered_set<uint32_t> _tablet_columns_convert_to_null_set;

    // ========= profiles ==========
    int64_t _compressed_bytes_read = 0;
    int64_t _raw_rows_read = 0;
    RuntimeProfile* _profile;
    bool _profile_updated = false;
};
} // namespace vectorized
} // namespace doris
