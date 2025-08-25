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

#include "vec/exec/executor/tools/simulator/split_generators.h"

#include <algorithm>

#include "vec/exec/executor/tools/simulator/split_specification.h"

namespace doris {
namespace vectorized {

// ======================== IntermediateSplitGenerator Implementation ========================

int64_t IntermediateSplitGenerator::_generate_intermediate_num_quanta(double origin,
                                                                      double bound) const {
    double value = std::uniform_real_distribution<>(origin, bound)(_gen);

    if (value > 0.95) {
        return std::uniform_int_distribution<int64_t>(2000, 20000)(_gen);
    }

    if (value > 0.90) {
        return std::uniform_int_distribution<int64_t>(1000, 2000)(_gen);
    }

    return std::uniform_int_distribution<int64_t>(10, 1000)(_gen);
}

int64_t IntermediateSplitGenerator::_generate_intermediate_scheduled_millis(double origin,
                                                                            double bound) const {
    double value = std::uniform_real_distribution<>(origin, bound)(_gen);
    // in reality, max is several hours, but this would make the simulation too slow

    if (value > 0.999) {
        return std::uniform_int_distribution<int64_t>(5 * 60 * 1000, 10 * 60 * 1000)(_gen);
    }

    if (value > 0.99) {
        return std::uniform_int_distribution<int64_t>(60 * 1000, 5 * 60 * 1000)(_gen);
    }

    if (value > 0.95) {
        return std::uniform_int_distribution<int64_t>(10000, 60 * 1000)(_gen);
    }

    if (value > 0.75) {
        return std::uniform_int_distribution<int64_t>(1000, 10000)(_gen);
    }

    if (value > 0.45) {
        return std::uniform_int_distribution<int64_t>(100, 1000)(_gen);
    }

    if (value > 0.20) {
        return std::uniform_int_distribution<int64_t>(10, 100)(_gen);
    }

    return std::uniform_int_distribution<int64_t>(1, 10)(_gen);
}

int64_t IntermediateSplitGenerator::_generate_intermediate_wall_millis(double origin,
                                                                       double bound) const {
    double value = std::uniform_real_distribution<>(origin, bound)(_gen);
    // in reality, max is several hours, but this would make the simulation too slow

    if (value > 0.90) {
        return std::uniform_int_distribution<int64_t>(400000, 800000)(_gen);
    }

    if (value > 0.75) {
        return std::uniform_int_distribution<int64_t>(100000, 200000)(_gen);
    }

    if (value > 0.50) {
        return std::uniform_int_distribution<int64_t>(50000, 100000)(_gen);
    }

    if (value > 0.40) {
        return std::uniform_int_distribution<int64_t>(30000, 50000)(_gen);
    }

    if (value > 0.30) {
        return std::uniform_int_distribution<int64_t>(20000, 30000)(_gen);
    }

    if (value > 0.20) {
        return std::uniform_int_distribution<int64_t>(10000, 15000)(_gen);
    }

    if (value > 0.10) {
        return std::uniform_int_distribution<int64_t>(5000, 10000)(_gen);
    }

    return std::uniform_int_distribution<int64_t>(1000, 5000)(_gen);
}

std::unique_ptr<SplitSpecification> IntermediateSplitGenerator::next() const {
    int64_t num_quanta = _generate_intermediate_num_quanta(0.0, 1.0);
    int64_t wall_nanos = _generate_intermediate_wall_millis(0.0, 1.0) * 1'000'000;
    int64_t scheduled_nanos = _generate_intermediate_scheduled_millis(0.0, 1.0) * 1'000'000;

    double ratio = std::uniform_real_distribution<>(0.97, 0.99)(_gen);
    int64_t blocked_nanos = static_cast<int64_t>(ratio * wall_nanos);
    blocked_nanos = std::min(blocked_nanos, wall_nanos - 1);

    num_quanta = num_quanta > 0 ? num_quanta : 1;

    int64_t per_quanta_nanos = scheduled_nanos / num_quanta;
    int64_t between_quanta_nanos = blocked_nanos / num_quanta;

    return std::make_unique<IntermediateSplitSpecification>(scheduled_nanos, wall_nanos, num_quanta,
                                                            per_quanta_nanos, between_quanta_nanos);
}

int64_t _generate_leaf_split_total_scheduled_millis(std::mt19937& gen, double origin,
                                                    double bound) {
    double value = std::uniform_real_distribution<>(origin, bound)(gen);
    if (value > 0.998) {
        return std::uniform_int_distribution<int64_t>(5 * 60 * 1000, 10 * 60 * 1000)(gen);
    }

    if (value > 0.99) {
        return std::uniform_int_distribution<int64_t>(60 * 1000, 5 * 60 * 1000)(gen);
    }

    if (value > 0.95) {
        return std::uniform_int_distribution<int64_t>(10000, 60 * 1000)(gen);
    }

    if (value > 0.50) {
        return std::uniform_int_distribution<int64_t>(1000, 10000)(gen);
    }

    if (value > 0.25) {
        return std::uniform_int_distribution<int64_t>(100, 1000)(gen);
    }

    if (value > 0.10) {
        return std::uniform_int_distribution<int64_t>(10, 100)(gen);
    }

    return std::uniform_int_distribution<int64_t>(1, 10)(gen);
}

int64_t _generate_leaf_split_quanta_micros(std::mt19937& gen, double origin, double bound) {
    double value = std::uniform_real_distribution<>(origin, bound)(gen);
    if (value > 0.9999) {
        return 200000000;
    }

    if (value > 0.99) {
        return std::uniform_int_distribution<int64_t>(3000000, 15000000)(gen);
    }

    if (value > 0.95) {
        return std::uniform_int_distribution<int64_t>(2000000, 5000000)(gen);
    }

    if (value > 0.90) {
        return std::uniform_int_distribution<int64_t>(1500000, 5000000)(gen);
    }

    if (value > 0.75) {
        return std::uniform_int_distribution<int64_t>(1000000, 2000000)(gen);
    }

    if (value > 0.50) {
        return std::uniform_int_distribution<int64_t>(500000, 1000000)(gen);
    }

    if (value > 0.1) {
        return std::uniform_int_distribution<int64_t>(100000, 500000)(gen);
    }

    return std::uniform_int_distribution<int64_t>(250, 500)(gen);
}

// ======================== AggregatedLeafSplitGenerator Implementation ========================

int64_t AggregatedLeafSplitGenerator::_generate_leaf_split_total_scheduled_millis(
        double origin, double bound) const {
    return ::doris::vectorized::_generate_leaf_split_total_scheduled_millis(_gen, origin, bound);
}

int64_t AggregatedLeafSplitGenerator::_generate_leaf_split_quanta_micros(double origin,
                                                                         double bound) const {
    return ::doris::vectorized::_generate_leaf_split_quanta_micros(_gen, origin, bound);
}

std::unique_ptr<SplitSpecification> AggregatedLeafSplitGenerator::next() const {
    int64_t total_nanos = _generate_leaf_split_total_scheduled_millis(0.0, 1.0) * 1'000'000;
    int64_t quanta_nanos =
            std::min(total_nanos, _generate_leaf_split_quanta_micros(0.0, 1.0) * 1'000);

    return std::make_unique<LeafSplitSpecification>(total_nanos, quanta_nanos);
}

// ======================== FastLeafSplitGenerator Implementation ========================

int64_t FastLeafSplitGenerator::_generate_leaf_split_total_scheduled_millis(double origin,
                                                                            double bound) const {
    return ::doris::vectorized::_generate_leaf_split_total_scheduled_millis(_gen, origin, bound);
}

int64_t FastLeafSplitGenerator::_generate_leaf_split_quanta_micros(double origin,
                                                                   double bound) const {
    return ::doris::vectorized::_generate_leaf_split_quanta_micros(_gen, origin, bound);
}

std::unique_ptr<SplitSpecification> FastLeafSplitGenerator::next() const {
    int64_t total_nanos = _generate_leaf_split_total_scheduled_millis(0.0, 0.75) * 1'000'000;
    int64_t quanta_nanos =
            std::min(total_nanos, _generate_leaf_split_quanta_micros(0.0, 1.0) * 1'000);

    return std::make_unique<LeafSplitSpecification>(total_nanos, quanta_nanos);
}

// ======================== SlowLeafSplitGenerator Implementation ========================

int64_t SlowLeafSplitGenerator::_generate_leaf_split_total_scheduled_millis(double origin,
                                                                            double bound) const {
    return ::doris::vectorized::_generate_leaf_split_total_scheduled_millis(_gen, origin, bound);
}

int64_t SlowLeafSplitGenerator::_generate_leaf_split_quanta_micros(double origin,
                                                                   double bound) const {
    return ::doris::vectorized::_generate_leaf_split_quanta_micros(_gen, origin, bound);
}

std::unique_ptr<SplitSpecification> SlowLeafSplitGenerator::next() const {
    int64_t total_nanos = _generate_leaf_split_total_scheduled_millis(0.75, 1.0) * 1'000'000;
    int64_t quanta_nanos =
            std::min(total_nanos, _generate_leaf_split_quanta_micros(0.0, 1.0) * 1'000);

    return std::make_unique<LeafSplitSpecification>(total_nanos, quanta_nanos);
}

// ======================== L4LeafSplitGenerator Implementation ========================

int64_t L4LeafSplitGenerator::_generate_leaf_split_total_scheduled_millis(double origin,
                                                                          double bound) const {
    return ::doris::vectorized::_generate_leaf_split_total_scheduled_millis(_gen, origin, bound);
}

int64_t L4LeafSplitGenerator::_generate_leaf_split_quanta_micros(double origin,
                                                                 double bound) const {
    return ::doris::vectorized::_generate_leaf_split_quanta_micros(_gen, origin, bound);
}

std::unique_ptr<SplitSpecification> L4LeafSplitGenerator::next() const {
    int64_t total_nanos = _generate_leaf_split_total_scheduled_millis(0.99, 1.0) * 1'000'000;
    int64_t quanta_nanos =
            std::min(total_nanos, _generate_leaf_split_quanta_micros(0.0, 0.9) * 1'000);

    return std::make_unique<LeafSplitSpecification>(total_nanos, quanta_nanos);
}

// ======================== QuantaExceedingSplitGenerator Implementation ========================

int64_t QuantaExceedingSplitGenerator::_generate_leaf_split_total_scheduled_millis(
        double origin, double bound) const {
    return ::doris::vectorized::_generate_leaf_split_total_scheduled_millis(_gen, origin, bound);
}

int64_t QuantaExceedingSplitGenerator::_generate_leaf_split_quanta_micros(double origin,
                                                                          double bound) const {
    return ::doris::vectorized::_generate_leaf_split_quanta_micros(_gen, origin, bound);
}

std::unique_ptr<SplitSpecification> QuantaExceedingSplitGenerator::next() const {
    int64_t total_nanos = _generate_leaf_split_total_scheduled_millis(0.99, 1.0) * 1'000'000;
    int64_t quanta_nanos =
            std::min(total_nanos, _generate_leaf_split_quanta_micros(0.75, 1.0) * 1'000);

    return std::make_unique<LeafSplitSpecification>(total_nanos, quanta_nanos);
}

// ======================== SimpleLeafSplitGenerator Implementation ========================

std::unique_ptr<SplitSpecification> SimpleLeafSplitGenerator::next() const {
    return std::make_unique<LeafSplitSpecification>(_total_nanos, _quanta_nanos);
}

} // namespace vectorized
} // namespace doris
