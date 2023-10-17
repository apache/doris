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

#include <gen_cpp/PaloInternalService_types.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "olap/data_dir.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {

struct OlapScanRange;
class FunctionFilter;
class RuntimeProfile;
class RuntimeState;
class TPaloScanRange;

namespace vectorized {

class NewOlapScanNode;
struct FilterPredicates;
class Block;

class NewOlapScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(NewOlapScanner);

public:
    NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit, bool aggregation,
                   const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   RuntimeProfile* profile);

    NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit, bool aggregation,
                   const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   TabletReader::ReadSource read_source, RuntimeProfile* profile);

    Status init() override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts);

    const std::string& scan_disk() const { return _tablet->data_dir()->path(); }

    void set_compound_filters(const std::vector<TCondition>& compound_filters);

    doris::TabletStorageType get_storage_type() override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _update_counters_before_close() override;

private:
    void _update_realtime_counters();

    Status _init_tablet_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                                      const std::vector<TCondition>& filters,
                                      const FilterPredicates& filter_predicates,
                                      const std::vector<FunctionFilter>& function_filters);

    [[nodiscard]] Status _init_return_columns();

    bool _aggregation;

    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;
    int64_t _version;
    const TPaloScanRange& _scan_range;
    std::vector<OlapScanRange*> _key_ranges;

    TabletReader::ReaderParams _tablet_reader_params;
    std::unique_ptr<TabletReader> _tablet_reader;

    std::vector<uint32_t> _return_columns;
    std::unordered_set<uint32_t> _tablet_columns_convert_to_null_set;
    std::vector<TCondition> _compound_filters;

    // ========= profiles ==========
    int64_t _compressed_bytes_read = 0;
    int64_t _raw_rows_read = 0;
    bool _profile_updated = false;
};
} // namespace vectorized
} // namespace doris
