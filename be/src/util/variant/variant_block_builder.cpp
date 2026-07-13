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

#include "util/variant/variant_block_builder.h"

#include <limits>
#include <utility>

#include "common/exception.h"
#include "util/variant/variant_scalar_encoding.h"

namespace doris {
namespace {

constexpr uint64_t FINISHED_ROW_GENERATION = std::numeric_limits<uint64_t>::max();
constexpr uint64_t ABORTED_ROW_GENERATION = FINISHED_ROW_GENERATION - 1;

} // namespace

VariantBlockBuilder::Row::ObjectScope::ObjectScope(VariantBlockBuilder* builder,
                                                   uint64_t generation, uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBlockBuilder::Row::ObjectScope::ObjectScope(ObjectScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBlockBuilder::Row::ArrayScope::ArrayScope(VariantBlockBuilder* builder, uint64_t generation,
                                                 uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBlockBuilder::Row::ArrayScope::ArrayScope(ArrayScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBlockBuilder::Row::Row(VariantBlockBuilder* builder, uint64_t generation) noexcept
        : _builder(builder), _generation(generation) {}

VariantBlockBuilder::Row::Row(Row&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)), _generation(other._generation) {}

void VariantBlockBuilder::Row::ObjectScope::add_key(StringRef key) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_add_key(_generation, _token, key);
}

void VariantBlockBuilder::Row::ObjectScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_finish_container(_generation, _token, true);
    _builder = nullptr;
}

void VariantBlockBuilder::Row::ArrayScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block array scope is already finished");
    }
    _builder->_finish_container(_generation, _token, false);
    _builder = nullptr;
}

VariantBlockBuilder::Row::~Row() {
    if (_builder != nullptr) {
        _builder->_abort_row_noexcept(_generation);
    }
}

void VariantBlockBuilder::Row::add_null() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_null(_generation);
}

void VariantBlockBuilder::Row::add_bool(bool value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_bool(_generation, value);
}

void VariantBlockBuilder::Row::add_int(int64_t value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_int(_generation, value);
}

void VariantBlockBuilder::Row::add_float(float value) {
    _add_scalar(VariantScalarEncodingPlan::float32(value));
}

void VariantBlockBuilder::Row::add_double(double value) {
    _add_scalar(VariantScalarEncodingPlan::float64(value));
}

void VariantBlockBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale));
}

void VariantBlockBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale, uint8_t width) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale, width));
}

void VariantBlockBuilder::Row::add_date(int32_t days_since_epoch) {
    _add_scalar(VariantScalarEncodingPlan::date(days_since_epoch));
}

void VariantBlockBuilder::Row::add_timestamp_micros(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_micros(value, utc_adjusted));
}

void VariantBlockBuilder::Row::add_timestamp_nanos(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_nanos(value, utc_adjusted));
}

void VariantBlockBuilder::Row::add_time_ntz_micros(int64_t value) {
    _add_scalar(VariantScalarEncodingPlan::time_ntz_micros(value));
}

void VariantBlockBuilder::Row::add_binary(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::binary(value));
}

void VariantBlockBuilder::Row::add_string(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::string(value));
}

void VariantBlockBuilder::Row::add_uuid(const std::array<uint8_t, 16>& value) {
    _add_scalar(VariantScalarEncodingPlan::uuid(value));
}

void VariantBlockBuilder::Row::add_largeint(__int128 value) {
    _add_scalar(VariantScalarEncodingPlan::largeint(value));
}

void VariantBlockBuilder::Row::add_value(VariantValueRef value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_value(_generation, value);
}

VariantBlockBuilder::Row::ObjectScope VariantBlockBuilder::Row::start_object() {
    const uint32_t token = _start_object();
    return {_builder, _generation, token};
}

VariantBlockBuilder::Row::ArrayScope VariantBlockBuilder::Row::start_array() {
    const uint32_t token = _start_array();
    return {_builder, _generation, token};
}

void VariantBlockBuilder::Row::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_finish_row(_generation);
    _generation = FINISHED_ROW_GENERATION;
}

void VariantBlockBuilder::Row::abort() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_abort_row(_generation);
    _generation = ABORTED_ROW_GENERATION;
}

bool VariantBlockBuilder::Row::is_finished() const noexcept {
    return _builder != nullptr && _generation == FINISHED_ROW_GENERATION;
}

void VariantBlockBuilder::Row::_add_scalar(const VariantScalarEncodingPlan& plan) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_scalar(_generation, plan);
}

uint32_t VariantBlockBuilder::Row::_start_object() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, true);
}

uint32_t VariantBlockBuilder::Row::_start_array() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, false);
}

} // namespace doris
