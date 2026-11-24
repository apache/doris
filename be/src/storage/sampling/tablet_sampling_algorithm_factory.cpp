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

#include "storage/sampling/tablet_sampling_algorithm_factory.h"

#include <bit>
#include <cmath>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/sampling/fixed_rowid_block_sampler.h"
#include "storage/sampling/key_range_block_sampler.h"

namespace doris {

Status TabletSamplingAlgorithmFactory::create(const TTabletSampleOptions& options,
                                              std::unique_ptr<TabletSamplingAlgorithm>* algorithm) {
    if (!options.__isset.protocol_version) {
        return Status::InvalidArgument("Missing tablet sample protocol version");
    }
    if (options.protocol_version != 1) {
        return Status::NotSupported("Unsupported tablet sample version {}",
                                    options.protocol_version);
    }
    if (!options.__isset.algorithm) {
        return Status::InvalidArgument("Missing tablet sample algorithm");
    }
    // Thrift stores an arbitrary wire i32 in an unscoped C++ enum. Reading an unknown value as
    // that enum is undefined; compare its wire representation before interpreting it.
    const auto algorithm_id = std::bit_cast<int32_t>(options.algorithm);
    const bool key_range = algorithm_id == TTabletSamplingAlgorithm::KEY_RANGE_BLOCK;
    if (!key_range && algorithm_id != TTabletSamplingAlgorithm::FIXED_ROWID_BLOCK) {
        return Status::NotSupported("Unsupported tablet sample algorithm {}", algorithm_id);
    }
    auto valid_probability = [](double p) { return std::isfinite(p) && p > 0 && p <= 1; };
    if (!options.__isset.intra_tablet_unit_probability || !options.__isset.seed ||
        !options.__isset.tablet_inclusion_probability ||
        options.__isset.fixed_rowid_block_params == key_range ||
        options.__isset.key_range_block_params != key_range ||
        !valid_probability(options.intra_tablet_unit_probability) ||
        !valid_probability(options.tablet_inclusion_probability)) {
        return Status::InvalidArgument("Invalid or missing fixed block sampling options");
    }
    auto create_sampler = [&]<typename Sampler>(const auto& params) {
        if (!params.__isset.sample_unit_rows || !params.__isset.max_sample_units_per_tablet ||
            params.sample_unit_rows <= 0 || params.max_sample_units_per_tablet < 0) {
            return Status::InvalidArgument("Invalid block sampling unit size or cap");
        }
        *algorithm = std::make_unique<Sampler>(options.intra_tablet_unit_probability, options.seed,
                                               params.sample_unit_rows,
                                               params.max_sample_units_per_tablet);
        return Status::OK();
    };
    if (key_range) {
        const auto& params = options.key_range_block_params;
        if (!params.__isset.sample_unit_rows || !params.__isset.max_sample_units_per_tablet ||
            params.sample_unit_rows <= 0 || params.max_sample_units_per_tablet < 0) {
            return Status::InvalidArgument("Invalid block sampling unit size or cap");
        }
        std::optional<KeyRangeSamplePlanSpec> shared_spec;
        if (params.__isset.shared_intervals || params.__isset.shared_population_units ||
            params.__isset.shared_selected_units) {
            if (!params.__isset.shared_intervals || !params.__isset.shared_population_units ||
                !params.__isset.shared_selected_units || params.shared_population_units < 0 ||
                params.shared_selected_units < 0 ||
                params.shared_selected_units > params.shared_population_units ||
                params.shared_intervals.size() >
                        static_cast<size_t>(params.shared_selected_units)) {
                return Status::InvalidArgument("Invalid shared key-range block sampling intervals");
            }
            KeyRangeSamplePlanSpec spec;
            spec.population_units = static_cast<uint64_t>(params.shared_population_units);
            spec.selected_units = static_cast<uint64_t>(params.shared_selected_units);
            spec.intervals.reserve(params.shared_intervals.size());
            auto parse_bound = [](const TKeyRangeBound& raw,
                                  std::optional<KeyRangeBound>* out) -> Status {
                if (!raw.__isset.prefix || raw.prefix.empty()) {
                    return Status::InvalidArgument("Empty shared key-range bound prefix");
                }
                KeyRangeBound bound;
                bound.prefix = raw.prefix;
                if (raw.__isset.tuple) {
                    bound.tuple = raw.tuple;
                }
                *out = std::move(bound);
                return Status::OK();
            };
            for (const auto& item : params.shared_intervals) {
                KeyRangeSampleInterval interval;
                if (item.__isset.lower) {
                    RETURN_IF_ERROR(parse_bound(item.lower, &interval.lower));
                }
                if (item.__isset.upper) {
                    RETURN_IF_ERROR(parse_bound(item.upper, &interval.upper));
                }
                spec.intervals.push_back(std::move(interval));
            }
            shared_spec = std::move(spec);
        }
        *algorithm = std::make_unique<KeyRangeBlockSampler>(
                options.intra_tablet_unit_probability, options.seed, params.sample_unit_rows,
                params.max_sample_units_per_tablet, std::move(shared_spec));
        return Status::OK();
    }
    return create_sampler.template operator()<FixedRowIdBlockSampler>(
            options.fixed_rowid_block_params);
}

} // namespace doris
