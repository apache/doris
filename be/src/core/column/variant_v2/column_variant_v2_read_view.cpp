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

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"

namespace doris {

namespace {

using MetadataIdsColumn = ColumnVector<TYPE_UINT32>;

} // namespace

ColumnVariantV2::ReadView::ReadView(const IColumn* metadatas, const IColumn* metadata_ids,
                                    const IColumn* values)
        : _metadatas(metadatas), _metadata_ids(metadata_ids), _values(values) {
    DORIS_CHECK(_metadatas != nullptr);
    DORIS_CHECK(_metadata_ids != nullptr);
    DORIS_CHECK(_values != nullptr);
    DORIS_CHECK_EQ(_metadata_ids->size(), _values->size())
            << "ColumnVariantV2 encoded read view row counts differ";
    _validated_metadata.assign(_metadatas->size(), 0);
}

ColumnVariantV2::ReadView::ReadView(const IColumn* typed, const DataTypePtr* typed_type)
        : _typed_state(true), _typed(typed), _typed_type(typed_type) {
    DORIS_CHECK(_typed != nullptr);
    DORIS_CHECK(_typed_type != nullptr);
    DORIS_CHECK(*_typed_type != nullptr);
}

size_t ColumnVariantV2::ReadView::size() const noexcept {
    return _typed_state ? _typed->size() : _values->size();
}

size_t ColumnVariantV2::ReadView::metadata_count() const noexcept {
    DORIS_CHECK(!_typed_state) << "metadata_count requires ColumnVariantV2 encoded state";
    return _metadatas->size();
}

uint32_t ColumnVariantV2::ReadView::metadata_id_at(size_t row) const {
    DORIS_CHECK(!_typed_state) << "metadata_id_at requires ColumnVariantV2 encoded state";
    DORIS_CHECK_LT(row, size()) << "ColumnVariantV2 encoded read row is out of range";
    const auto& ids = assert_cast<const MetadataIdsColumn&>(*_metadata_ids).get_data();
    const uint32_t id = ids[row];
    DORIS_CHECK_LT(id, metadata_count())
            << "ColumnVariantV2 encoded read metadata id is out of range";
    return id;
}

VariantMetadataRef ColumnVariantV2::ReadView::metadata_at(uint32_t id) const {
    DORIS_CHECK(!_typed_state) << "metadata_at requires ColumnVariantV2 encoded state";
    DORIS_CHECK_LT(id, metadata_count()) << "ColumnVariantV2 encoded read metadata is out of range";
    const StringRef metadata = assert_cast<const ColumnString&>(*_metadatas).get_data_at(id);
    VariantMetadataRef result {.data = metadata.data, .size = metadata.size};
    if (_validated_metadata[id] == 0) {
        result.validate();
        _validated_metadata[id] = 1;
    }
    return result;
}

VariantRef ColumnVariantV2::ReadView::value_at(size_t row) const {
    DORIS_CHECK(!_typed_state) << "value_at requires ColumnVariantV2 encoded state";
    const uint32_t metadata_id = metadata_id_at(row);
    const StringRef value = assert_cast<const ColumnString&>(*_values).get_data_at(row);
    VariantRef result {.metadata = metadata_at(metadata_id), .value = value};
    const size_t encoded_size = result.value_size();
    if (encoded_size != result.value.size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant value has {} trailing bytes",
                        result.value.size - encoded_size);
    }
    return result;
}

const IColumn& ColumnVariantV2::ReadView::typed_column() const {
    DORIS_CHECK(_typed_state) << "typed_column requires ColumnVariantV2 typed state";
    return *_typed;
}

const DataTypePtr& ColumnVariantV2::ReadView::typed_type() const {
    DORIS_CHECK(_typed_state) << "typed_type requires ColumnVariantV2 typed state";
    return *_typed_type;
}

ColumnVariantV2::ReadView ColumnVariantV2::read_view() const {
    if (_typed) {
        DORIS_CHECK(_typed_type != nullptr) << "typed state requires a data type";
        return {static_cast<const IColumn::Ptr&>(_typed).get(), &_typed_type};
    }
    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";
    return {static_cast<const IColumn::Ptr&>(_metadatas).get(),
            static_cast<const IColumn::Ptr&>(_meta_ids).get(),
            static_cast<const IColumn::Ptr&>(_values).get()};
}

} // namespace doris
