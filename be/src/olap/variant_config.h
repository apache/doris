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

#include <cstdint>
#include <limits>
#include <string>

namespace doris {

class TVariantConfig;
class VariantConfigPB;

class VariantConfig {
public:
    VariantConfig() = default;
    VariantConfig(const VariantConfig&) = default;
    VariantConfig& operator=(const VariantConfig&) = default;
    VariantConfig(VariantConfig&&) = default;
    VariantConfig& operator=(VariantConfig&&) = default;
    ~VariantConfig() = default;

    bool is_enable_decimal_type() const { return _enable_decimal_type; }
    void set_enable_decimal_type(bool enable_decimal_type) {
        _enable_decimal_type = enable_decimal_type;
    }

    double ratio_of_defaults_as_sparse_column() const {
        return _ratio_of_defaults_as_sparse_column;
    }
    void set_ratio_of_defaults_as_sparse_column(double ratio_of_defaults_as_sparse_column) {
        _ratio_of_defaults_as_sparse_column = ratio_of_defaults_as_sparse_column;
    }
    double threshold_rows_to_estimate_sparse_column() const {
        return _threshold_rows_to_estimate_sparse_column;
    }
    void set_threshold_rows_to_estimate_sparse_column(
            double threshold_rows_to_estimate_sparse_column) {
        _threshold_rows_to_estimate_sparse_column = threshold_rows_to_estimate_sparse_column;
    }

    VariantConfig& operator=(const TVariantConfig& config);
    VariantConfig& operator=(const VariantConfigPB& config);

    void to_pb(VariantConfigPB* config_pb) const;
    std::string to_string() const;

private:
    bool _enable_decimal_type {false};
    double _ratio_of_defaults_as_sparse_column {1.0};
    long _threshold_rows_to_estimate_sparse_column {1024};
};

} // namespace doris
