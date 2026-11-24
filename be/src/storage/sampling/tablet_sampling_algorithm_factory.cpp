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
        return create_sampler.template operator()<KeyRangeBlockSampler>(
                options.key_range_block_params);
    }
    return create_sampler.template operator()<FixedRowIdBlockSampler>(
            options.fixed_rowid_block_params);
}

} // namespace doris
