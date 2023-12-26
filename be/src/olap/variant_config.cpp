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

#include "olap/variant_config.h"

#include <fmt/format.h>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"

namespace doris {
VariantConfig& VariantConfig::operator=(const TVariantConfig& config) {
    if (config.__isset.enable_decimal_type) {
        _enable_decimal_type = config.enable_decimal_type;
    }
    if (config.__isset.ratio_of_defaults_as_sparse_column) {
        _ratio_of_defaults_as_sparse_column = config.ratio_of_defaults_as_sparse_column;
    }
    if (config.__isset.threshold_rows_to_estimate_sparse_column) {
        _threshold_rows_to_estimate_sparse_column = config.threshold_rows_to_estimate_sparse_column;
    }
    return *this;
}

VariantConfig& VariantConfig::operator=(const VariantConfigPB& config) {
    if (config.has_enable_decimal_type()) {
        _enable_decimal_type = config.enable_decimal_type();
    }
    if (config.has_ratio_of_defaults_as_sparse_column()) {
        _ratio_of_defaults_as_sparse_column = config.ratio_of_defaults_as_sparse_column();
    }
    if (config.has_threshold_rows_to_estimate_sparse_column()) {
        _threshold_rows_to_estimate_sparse_column =
                config.threshold_rows_to_estimate_sparse_column();
    }
    return *this;
}

void VariantConfig::to_pb(VariantConfigPB* config_pb) const {
    config_pb->set_enable_decimal_type(_enable_decimal_type);
    config_pb->set_ratio_of_defaults_as_sparse_column(_ratio_of_defaults_as_sparse_column);
    config_pb->set_threshold_rows_to_estimate_sparse_column(
            _threshold_rows_to_estimate_sparse_column);
}

std::string VariantConfig::to_string() const {
    return fmt::format(
            "VariantConfig enable_decimal_type: {}, ratio_of_defaults_as_sparse_column: {}, "
            "threshold_rows_to_estimate_sparse_column: {}",
            _enable_decimal_type, _ratio_of_defaults_as_sparse_column,
            _threshold_rows_to_estimate_sparse_column);
}

} // namespace doris
