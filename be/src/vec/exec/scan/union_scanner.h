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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "vec/exec/scan/new_olap_scanner.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

/**
 `UnionScanner` contains more than 1 `NewOlapScanner`
 */
class UnionScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(UnionScanner)
public:
    UnionScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t limit,
                 RuntimeProfile* profile)
            : VScanner(state, local_state, limit, profile) {
        _is_init = false;
    }

    void add_scanner(std::shared_ptr<NewOlapScanner> scanners) {
        _olap_scanners.emplace_back(std::move(scanners));
    }

    [[nodiscard]] std::shared_ptr<NewOlapScanner> get_single_scanner() {
        DCHECK_EQ(_olap_scanners.size(), 1);
        return std::move(_olap_scanners[0]);
    }

    [[nodiscard]] size_t get_scanners_count() const { return _olap_scanners.size(); }

    Status init() override;

    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    void set_compound_filters(const std::vector<TCondition>& compound_filters) override;

    doris::TabletStorageType get_storage_type() override;

    std::string get_name() override { return "UnionScanner"; }

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _collect_profile_before_close() override;

private:
    size_t _scanner_cursor {0};
    std::vector<std::shared_ptr<NewOlapScanner>> _olap_scanners;
};

} // namespace doris::vectorized
