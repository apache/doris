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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "format_v2/jni/jni_table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/options.h"

namespace doris::format::paimon {

class PaimonJniReader : public format::JniTableReader {
public:
    ~PaimonJniReader() override = default;
    Status close() override;

#ifdef BE_TEST
    void TEST_set_scan_params(TFileScanRangeParams* params) { _scan_params = params; }
    void TEST_set_runtime_state(RuntimeState* state) { _runtime_state = state; }
    void TEST_set_current_range(TFileRangeDesc range) { _current_range = std::move(range); }
    Status TEST_build_scanner_params(std::map<std::string, std::string>* params) const {
        return build_scanner_params(params);
    }
    static std::string TEST_build_default_io_manager_tmp_dirs(
            const std::vector<StorePath>& store_paths) {
        return build_default_io_manager_tmp_dirs(store_paths);
    }
    void TEST_set_current_split_prepared(bool prepared) { _current_split_prepared = prepared; }
    bool TEST_current_split_prepared() const { return _current_split_prepared; }
#endif

protected:
    std::string connector_class() const override;
    Status validate_scan_range(const TFileRangeDesc& range) const override;
    Status build_scanner_params(std::map<std::string, std::string>* params) const override;
    bool supports_batch_size_update_after_open() const override { return false; }
    Status open_jni_scanner_for_split() override;
    Status close_jni_scanner_for_split() override;
    void _on_jni_scanner_discarded() override { _current_split_prepared = false; }

    Status _prepare_for_split();
    virtual Status _reset_current_split();

private:
    static std::string build_default_io_manager_tmp_dirs(const std::vector<StorePath>& store_paths);

    bool _current_split_prepared = false;
};

} // namespace doris::format::paimon
