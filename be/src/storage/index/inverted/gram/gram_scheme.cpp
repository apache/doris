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

#include "storage/index/inverted/gram/gram_scheme.h"

#include <fmt/format.h>

#include <cmath>
#include <cstdlib>

namespace doris::segment_v2::gram {

namespace {
// Parse an integer property whose value range is [lo, hi]; on failure return InvalidArgument
// with context.
Status parse_uint(const std::string& key, const std::string& v, uint32_t lo, uint32_t hi,
                  uint32_t* out) {
    char* end = nullptr;
    long x = std::strtol(v.c_str(), &end, 10);
    if (end == v.c_str() || *end != '\0' || x < (long)lo || x > (long)hi) {
        return Status::InvalidArgument("gram property {}={} out of range [{},{}]", key, v, lo, hi);
    }
    *out = (uint32_t)x;
    return Status::OK();
}

// Parse a permille property: the input is a decimal in [lo, hi] (0.33, say) and is stored as
// that value times 1000 (330).
Status parse_permille(const std::string& key, const std::string& v, double lo, double hi,
                      uint32_t* out) {
    char* end = nullptr;
    double x = std::strtod(v.c_str(), &end);
    if (end == v.c_str() || *end != '\0' || !(x >= lo && x <= hi)) {
        return Status::InvalidArgument("gram property {}={} out of range [{},{}]", key, v, lo, hi);
    }
    *out = (uint32_t)std::lround(x * 1000.0);
    return Status::OK();
}
} // namespace

Status GramScheme::from_properties(const std::map<std::string, std::string>& props,
                                   GramScheme* out) {
    GramScheme s;
    if (auto it = props.find("mode"); it != props.end()) {
        if (it->second == "sparse" || it->second == "auto") {
            s.mode = GramMode::SPARSE; // auto resolves per segment sample; sparse is the default
        } else if (it->second == "dense") {
            s.mode = GramMode::DENSE;
        } else {
            return Status::InvalidArgument("gram property mode={} must be auto|sparse|dense",
                                           it->second);
        }
    }
    if (auto it = props.find("min_gram"); it != props.end()) {
        RETURN_IF_ERROR(parse_uint("min_gram", it->second, 1, 64, &s.min_len));
    }
    if (auto it = props.find("max_gram"); it != props.end()) {
        RETURN_IF_ERROR(parse_uint("max_gram", it->second, 1, 256, &s.max_len));
    }
    if (s.max_len < s.min_len) {
        return Status::InvalidArgument("gram property max_gram({}) < min_gram({})", s.max_len,
                                       s.min_len);
    }
    if (auto it = props.find("density"); it != props.end()) {
        RETURN_IF_ERROR(parse_permille("density", it->second, 0.001, 1.0, &s.density_permille));
    }
    if (auto it = props.find("stop_gram_df"); it != props.end()) {
        RETURN_IF_ERROR(parse_permille("stop_gram_df", it->second, 0.0, 1.0, &s.stop_df_permille));
    }
    if (auto it = props.find("lower_case"); it != props.end()) {
        if (it->second == "true" || it->second == "1") {
            s.lower_case = true;
        } else if (it->second == "false" || it->second == "0") {
            s.lower_case = false;
        } else {
            return Status::InvalidArgument("gram property lower_case={} must be true|false",
                                           it->second);
        }
    }
    if (auto it = props.find("hash_version"); it != props.end()) {
        RETURN_IF_ERROR(parse_uint("hash_version", it->second, 1, 1, &s.hash_version));
    }
    *out = s;
    return Status::OK();
}

std::map<std::string, std::string> GramScheme::to_properties() const {
    return {{"mode", mode == GramMode::DENSE ? "dense" : "sparse"},
            {"min_gram", std::to_string(min_len)},
            {"max_gram", std::to_string(max_len)},
            {"density", fmt::format("{:.3f}", density_permille / 1000.0)},
            {"stop_gram_df", fmt::format("{:.3f}", stop_df_permille / 1000.0)},
            {"lower_case", lower_case ? "true" : "false"},
            {"hash_version", std::to_string(hash_version)}};
}

std::string GramScheme::cache_key() const {
    return fmt::format("gram:v{}:{}:{}:{}:{}:{}:lc{}", hash_version,
                       mode == GramMode::DENSE ? "dense" : "sparse", min_len, max_len,
                       density_permille, stop_df_permille, lower_case ? 1 : 0);
}

} // namespace doris::segment_v2::gram
