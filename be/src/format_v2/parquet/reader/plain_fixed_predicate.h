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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "common/status.h"
#include "util/unaligned.h"

namespace doris::format::parquet {

enum class PlainFixedPredicateOp : uint8_t { EQ, NE, LT, LE, GT, GE };
enum class PlainFixedPredicateType : uint8_t { INT32, INT64, FLOAT, DOUBLE };

// A compiled predicate over the physical little-endian values of a fixed-width PLAIN page.
// Compilation is deliberately restricted to expressions whose Doris value and Parquet physical
// value have identical comparison semantics; casts and logical-type conversions stay on the
// ordinary materialization path.
class PlainFixedPredicate {
public:
    template <typename T>
    static PlainFixedPredicate create(PlainFixedPredicateType type, PlainFixedPredicateOp op,
                                      T literal) {
        PlainFixedPredicate predicate;
        predicate._type = type;
        predicate._op = op;
        static_assert(sizeof(T) <= sizeof(predicate._literal));
        memcpy(predicate._literal.data(), &literal, sizeof(T));
        return predicate;
    }

    size_t value_width() const {
        switch (_type) {
        case PlainFixedPredicateType::INT32:
        case PlainFixedPredicateType::FLOAT:
            return sizeof(uint32_t);
        case PlainFixedPredicateType::INT64:
        case PlainFixedPredicateType::DOUBLE:
            return sizeof(uint64_t);
        }
        __builtin_unreachable();
    }

    PlainFixedPredicateType type() const { return _type; }

    // AND this predicate into matches. The caller owns NULL handling because Parquet omits NULLs
    // from the physical value stream.
    Status evaluate(const uint8_t* values, size_t num_values, size_t value_width,
                    uint8_t* matches) const {
        if (UNLIKELY(value_width != this->value_width())) {
            return Status::Corruption("PLAIN predicate width {} does not match expected {}",
                                      value_width, this->value_width());
        }
        switch (_type) {
        case PlainFixedPredicateType::INT32:
            return _evaluate<int32_t>(values, num_values, matches);
        case PlainFixedPredicateType::INT64:
            return _evaluate<int64_t>(values, num_values, matches);
        case PlainFixedPredicateType::FLOAT:
            return _evaluate<float>(values, num_values, matches);
        case PlainFixedPredicateType::DOUBLE:
            return _evaluate<double>(values, num_values, matches);
        }
        __builtin_unreachable();
    }

private:
    template <typename T>
    Status _evaluate(const uint8_t* values, size_t num_values, uint8_t* matches) const {
        const T literal = unaligned_load<T>(_literal.data());
        for (size_t row = 0; row < num_values; ++row) {
            if (matches[row] == 0) {
                continue;
            }
            const T value = unaligned_load<T>(values + row * sizeof(T));
            bool keep = false;
            switch (_op) {
            case PlainFixedPredicateOp::EQ:
                keep = value == literal;
                break;
            case PlainFixedPredicateOp::NE:
                keep = value != literal;
                break;
            case PlainFixedPredicateOp::LT:
                keep = value < literal;
                break;
            case PlainFixedPredicateOp::LE:
                keep = value <= literal;
                break;
            case PlainFixedPredicateOp::GT:
                keep = value > literal;
                break;
            case PlainFixedPredicateOp::GE:
                keep = value >= literal;
                break;
            }
            matches[row] = static_cast<uint8_t>(keep);
        }
        return Status::OK();
    }

    PlainFixedPredicateType _type = PlainFixedPredicateType::INT32;
    PlainFixedPredicateOp _op = PlainFixedPredicateOp::EQ;
    std::array<uint8_t, sizeof(uint64_t)> _literal {};
};

} // namespace doris::format::parquet
