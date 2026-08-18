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

#include "core/value/variant/variant_selected_value_builder.h"

#include <utility>

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_scalar.h"

namespace doris {

VariantSelectedValueBuilder::VariantSelectedValueBuilder(size_t reserve_rows)
        : _reserve_rows(reserve_rows),
          _missing(ColumnUInt8::create()),
          _variant_nulls(ColumnUInt8::create()) {
    _missing->reserve(reserve_rows);
    _variant_nulls->reserve(reserve_rows);
}

VariantSelectedValueBuilder::~VariantSelectedValueBuilder() = default;

PrimitiveType VariantSelectedValueBuilder::typed_identity() const noexcept {
    return _type == nullptr ? INVALID_TYPE : _type->get_primitive_type();
}

VariantSelectedValueBuilder::Kind VariantSelectedValueBuilder::kind_of(VariantRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return Kind::STRING;
    case VariantBasicType::OBJECT:
    case VariantBasicType::ARRAY:
        return Kind::UNSUPPORTED;
    case VariantBasicType::PRIMITIVE:
        break;
    }

    switch (value.primitive_id()) {
    case VariantPrimitiveId::STRING:
        return Kind::STRING;
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return Kind::BOOLEAN;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return Kind::INTEGER;
    case VariantPrimitiveId::FLOAT:
        return Kind::FLOAT;
    case VariantPrimitiveId::DOUBLE:
        return Kind::DOUBLE;
    default:
        // Decimal width, temporal unit, and binary/UUID annotations either have no exact
        // ColumnVariantV2 typed identity or would not survive a typed round trip unchanged.
        return Kind::UNSUPPORTED;
    }
}

DataTypePtr VariantSelectedValueBuilder::identity_type(Kind kind) {
    switch (kind) {
    case Kind::STRING:
        return std::make_shared<DataTypeString>();
    case Kind::BOOLEAN:
        return std::make_shared<DataTypeBool>();
    case Kind::INTEGER:
        return std::make_shared<DataTypeInt64>();
    case Kind::FLOAT:
        return std::make_shared<DataTypeFloat32>();
    case Kind::DOUBLE:
        return std::make_shared<DataTypeFloat64>();
    case Kind::UNDECIDED:
    case Kind::UNSUPPORTED:
        break;
    }
    DORIS_CHECK(false) << "Variant selected value kind has no typed identity";
    return nullptr;
}

void VariantSelectedValueBuilder::_start_encoded() {
    _encoded = std::make_unique<VariantBatchBuilder>(
            VariantBatchBuilder::ReserveHint {.rows = _reserve_rows});
}

void VariantSelectedValueBuilder::_append_typed_null() {
    if (_values) {
        _values->insert_default();
    }
    _variant_nulls->insert_value(1);
}

bool VariantSelectedValueBuilder::_try_append_typed(VariantRef value) {
    switch (_kind) {
    case Kind::STRING: {
        const StringRef text = value.get_string();
        assert_cast<ColumnString&>(*_values).insert_data(text.data, text.size);
        return true;
    }
    case Kind::BOOLEAN:
        assert_cast<ColumnUInt8&>(*_values).insert_value(value.get_bool() ? 1 : 0);
        return true;
    case Kind::INTEGER: {
        const int64_t scalar = value.get_int();
        // A typed BIGINT column re-encodes through the narrowest signed width, so a leaf written
        // wider than its value needs would change the observable Variant type. Keep those rows on
        // the canonical encoded path instead.
        if (VariantScalarRef::integer(scalar).encoded_size() != value.value_size()) {
            return false;
        }
        assert_cast<ColumnInt64&>(*_values).insert_value(scalar);
        return true;
    }
    case Kind::FLOAT:
        assert_cast<ColumnFloat32&>(*_values).insert_value(value.get_float());
        return true;
    case Kind::DOUBLE:
        assert_cast<ColumnFloat64&>(*_values).insert_value(value.get_double());
        return true;
    case Kind::UNDECIDED:
    case Kind::UNSUPPORTED:
        break;
    }
    DORIS_CHECK(false) << "Variant selected value kind has no typed append";
    return false;
}

void VariantSelectedValueBuilder::_replay_typed_scalar(size_t row,
                                                       VariantBatchBuilder* encoded) const {
    // Each case must encode exactly what with_variant_typed_scalar() would emit for this typed
    // identity, so degrading cannot change values that were already accepted as typed.
    auto output = encoded->begin_row();
    switch (_kind) {
    case Kind::STRING: {
        const StringRef text = assert_cast<const ColumnString&>(*_values).get_data_at(row);
        output.add_string(text);
        break;
    }
    case Kind::BOOLEAN:
        output.add_bool(assert_cast<const ColumnUInt8&>(*_values).get_data()[row] != 0);
        break;
    case Kind::INTEGER:
        output.add_int(assert_cast<const ColumnInt64&>(*_values).get_data()[row]);
        break;
    case Kind::FLOAT:
        output.add_float(assert_cast<const ColumnFloat32&>(*_values).get_data()[row]);
        break;
    case Kind::DOUBLE:
        output.add_double(assert_cast<const ColumnFloat64&>(*_values).get_data()[row]);
        break;
    case Kind::UNDECIDED:
    case Kind::UNSUPPORTED:
        DORIS_CHECK(false) << "Variant selected value kind has no encoded replay";
        break;
    }
    output.finish();
}

void VariantSelectedValueBuilder::_degrade() {
    DORIS_CHECK(_mode == Mode::TYPED) << "Variant selected value builder already degraded";
    _start_encoded();
    const NullMap& missing = _missing->get_data();
    const NullMap& variant_nulls = _variant_nulls->get_data();
    DORIS_CHECK_EQ(missing.size(), _rows);
    DORIS_CHECK_EQ(variant_nulls.size(), _rows);
    for (size_t row = 0; row < _rows; ++row) {
        if (missing[row] != 0 || variant_nulls[row] != 0) {
            auto output = _encoded->begin_row();
            output.add_null();
            output.finish();
            continue;
        }
        _replay_typed_scalar(row, _encoded.get());
    }
    _values.reset();
    _variant_nulls->clear();
    _type.reset();
    _kind = Kind::UNSUPPORTED;
    _mode = Mode::ENCODED;
}

void VariantSelectedValueBuilder::_append_encoded(VariantRef value) {
    auto output = _encoded->begin_row();
    output.add_value(value);
    output.finish();
}

void VariantSelectedValueBuilder::append_missing() {
    if (_mode == Mode::ENCODED) {
        auto output = _encoded->begin_row();
        output.add_null();
        output.finish();
    } else {
        _append_typed_null();
    }
    _missing->insert_value(1);
    ++_rows;
}

void VariantSelectedValueBuilder::append_selected(VariantRef value) {
    if (_mode == Mode::TYPED) {
        if (value.is_null()) {
            // A Variant null carries no scalar kind, so it never decides the typed identity.
            _append_typed_null();
            _missing->insert_value(0);
            ++_rows;
            return;
        }
        const Kind kind = kind_of(value);
        if (kind != Kind::UNSUPPORTED && (_kind == Kind::UNDECIDED || kind == _kind)) {
            if (_kind == Kind::UNDECIDED) {
                _kind = kind;
                _type = identity_type(kind);
                _values = _type->create_column();
                _values->reserve(_reserve_rows);
                // Rows appended before an identity existed are missing or Variant null, so their
                // typed slots are never observed.
                _values->insert_many_defaults(_rows);
            }
            if (_try_append_typed(value)) {
                _variant_nulls->insert_value(0);
                _missing->insert_value(0);
                ++_rows;
                return;
            }
        }
        _degrade();
    }
    _append_encoded(value);
    _missing->insert_value(0);
    ++_rows;
}

ColumnPtr VariantSelectedValueBuilder::finish() {
    if (_mode == Mode::TYPED && _kind == Kind::UNDECIDED) {
        // No row selected a scalar, so there is no identity to publish as a typed column.
        _degrade();
    }
    if (_mode == Mode::ENCODED) {
        auto values = ColumnVariantV2::create();
        values->insert_encoded_batch(_encoded->finish_batch());
        DORIS_CHECK_EQ(values->size(), _rows);
        return ColumnNullable::create(std::move(values), std::move(_missing));
    }

    DORIS_CHECK_EQ(_values->size(), _rows);
    auto typed = ColumnNullable::create(std::move(_values), std::move(_variant_nulls));
    auto values = ColumnVariantV2::create_typed(std::move(typed), _type);
    return ColumnNullable::create(std::move(values), std::move(_missing));
}

} // namespace doris
