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
#include <random>

#include "vec/exec/executor/tools/simulator/split_specification.h"

namespace doris {
namespace vectorized {

class SplitGenerator {
public:
    virtual ~SplitGenerator() = default;
    virtual std::unique_ptr<SplitSpecification> next() const = 0;
};

class IntermediateSplitGenerator : public SplitGenerator {
public:
    explicit IntermediateSplitGenerator() : _gen(std::random_device {}()) {}

    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_intermediate_num_quanta(double origin = 0.0, double bound = 1.0) const;
    int64_t _generate_intermediate_scheduled_millis(double origin = 0.0, double bound = 1.0) const;
    int64_t _generate_intermediate_wall_millis(double origin = 0.0, double bound = 1.0) const;

    mutable std::mt19937 _gen;
};

class AggregatedLeafSplitGenerator : public SplitGenerator {
public:
    explicit AggregatedLeafSplitGenerator() : _gen(std::random_device {}()) {}
    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_leaf_split_total_scheduled_millis(double origin = 0.0,
                                                        double bound = 1.0) const;
    int64_t _generate_leaf_split_quanta_micros(double origin = 0.0, double bound = 1.0) const;

    mutable std::mt19937 _gen;
};

class FastLeafSplitGenerator : public SplitGenerator {
public:
    explicit FastLeafSplitGenerator() : _gen(std::random_device {}()) {}
    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_leaf_split_total_scheduled_millis(double origin = 0.0,
                                                        double bound = 0.75) const;
    int64_t _generate_leaf_split_quanta_micros(double origin = 0.0, double bound = 1.0) const;

    mutable std::mt19937 _gen;
};

class SlowLeafSplitGenerator : public SplitGenerator {
public:
    explicit SlowLeafSplitGenerator() : _gen(std::random_device {}()) {}
    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_leaf_split_total_scheduled_millis(double origin = 0.75,
                                                        double bound = 1.0) const;
    int64_t _generate_leaf_split_quanta_micros(double origin = 0.0, double bound = 1.0) const;

    mutable std::mt19937 _gen;
};

class L4LeafSplitGenerator : public SplitGenerator {
public:
    explicit L4LeafSplitGenerator() : _gen(std::random_device {}()) {}

    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_leaf_split_total_scheduled_millis(double origin = 0.99,
                                                        double bound = 1.0) const;
    int64_t _generate_leaf_split_quanta_micros(double origin = 0.0, double bound = 0.9) const;

    mutable std::mt19937 _gen;
};

class QuantaExceedingSplitGenerator : public SplitGenerator {
public:
    explicit QuantaExceedingSplitGenerator() : _gen(std::random_device {}()) {}

    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _generate_leaf_split_total_scheduled_millis(double origin = 0.99,
                                                        double bound = 1.0) const;
    int64_t _generate_leaf_split_quanta_micros(double origin = 0.75, double bound = 1.0) const;

    mutable std::mt19937 _gen;
};

class SimpleLeafSplitGenerator : public SplitGenerator {
public:
    SimpleLeafSplitGenerator(int64_t total_nanos, int64_t quanta_nanos)
            : _total_nanos(total_nanos), _quanta_nanos(quanta_nanos) {}

    std::unique_ptr<SplitSpecification> next() const override;

private:
    int64_t _total_nanos;
    int64_t _quanta_nanos;
};

} // namespace vectorized
} // namespace doris
