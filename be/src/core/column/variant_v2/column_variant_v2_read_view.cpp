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

#include "core/column/variant_v2/column_variant_v2.h"

namespace doris {

ColumnVariantV2::ReadView::ReadView(const ColumnString* metadatas,
                                    const MetadataIdsColumn* metadata_ids,
                                    const ColumnString* values)
        : _metadatas(metadatas), _metadata_ids(metadata_ids), _values(values) {
    DORIS_CHECK(_metadatas != nullptr);
    DORIS_CHECK(_metadata_ids != nullptr);
    DORIS_CHECK(_values != nullptr);
    DORIS_CHECK_EQ(_metadata_ids->size(), _values->size())
            << "ColumnVariantV2 encoded read view row counts differ";
}

ColumnVariantV2::ReadView::ReadView(const IColumn* typed, const DataTypePtr* typed_type)
        : _representation(Representation::TYPED_SCALAR), _typed(typed), _typed_type(typed_type) {
    DORIS_CHECK(_typed != nullptr);
    DORIS_CHECK(_typed_type != nullptr);
    DORIS_CHECK(*_typed_type != nullptr);
}

ColumnVariantV2::ReadView::ReadView(const ColumnString* metadatas,
                                    const MetadataIdsColumn* metadata_ids,
                                    const ColumnString* values,
                                    const ShreddedFields* shredded_fields)
        : _representation(Representation::SHREDDED),
          _metadatas(metadatas),
          _metadata_ids(metadata_ids),
          _values(values),
          _shredded_fields(shredded_fields) {
    DORIS_CHECK(_metadatas != nullptr);
    DORIS_CHECK(_metadata_ids != nullptr);
    DORIS_CHECK(_values != nullptr);
    DORIS_CHECK(_shredded_fields != nullptr);
    DORIS_CHECK(!_shredded_fields->empty());
    DORIS_CHECK_EQ(_metadata_ids->size(), _values->size())
            << "ColumnVariantV2 residual read view row counts differ";
}

size_t ColumnVariantV2::ReadView::size() const noexcept {
    return is_typed() ? _typed->size() : _values->size();
}

size_t ColumnVariantV2::ReadView::metadata_count() const noexcept {
    DORIS_CHECK(is_encoded()) << "metadata_count requires ColumnVariantV2 encoded state";
    return _metadatas->size();
}

uint32_t ColumnVariantV2::ReadView::metadata_id_at(size_t row) const {
    DORIS_CHECK(is_encoded()) << "metadata_id_at requires ColumnVariantV2 encoded state";
    DORIS_CHECK_LT(row, size()) << "ColumnVariantV2 encoded read row is out of range";
    const auto& ids = _metadata_ids->get_data();
    const uint32_t id = ids[row];
    DORIS_CHECK_LT(id, metadata_count())
            << "ColumnVariantV2 encoded read metadata id is out of range";
    return id;
}

VariantMetadataRef ColumnVariantV2::ReadView::metadata_at(uint32_t id) const {
    DORIS_CHECK(is_encoded()) << "metadata_at requires ColumnVariantV2 encoded state";
    DORIS_CHECK_LT(id, metadata_count()) << "ColumnVariantV2 encoded read metadata is out of range";
    const StringRef metadata = _metadatas->get_data_at(id);
    return {.data = metadata.data, .size = metadata.size};
}

VariantRef ColumnVariantV2::ReadView::value_at(size_t row) const {
    DORIS_CHECK(is_encoded()) << "value_at requires ColumnVariantV2 encoded state";
    const uint32_t metadata_id = metadata_id_at(row);
    const StringRef value = _values->get_data_at(row);
    return {.metadata = metadata_at(metadata_id), .value = value};
}

const IColumn& ColumnVariantV2::ReadView::typed_column() const {
    DORIS_CHECK(is_typed()) << "typed_column requires ColumnVariantV2 typed state";
    return *_typed;
}

const DataTypePtr& ColumnVariantV2::ReadView::typed_type() const {
    DORIS_CHECK(is_typed()) << "typed_type requires ColumnVariantV2 typed state";
    return *_typed_type;
}

size_t ColumnVariantV2::ReadView::shredded_field_count() const noexcept {
    DORIS_CHECK(is_shredded()) << "shredded_field_count requires ColumnVariantV2 shredded state";
    return _shredded_fields->size();
}

const PathInData& ColumnVariantV2::ReadView::shredded_field_path(size_t index) const {
    DORIS_CHECK_LT(index, shredded_field_count()) << "shredded field index is out of range";
    return (*_shredded_fields)[index].path;
}

const ColumnVariantV2& ColumnVariantV2::ReadView::shredded_field_values(size_t index) const {
    DORIS_CHECK_LT(index, shredded_field_count()) << "shredded field index is out of range";
    return assert_cast<const ColumnVariantV2&>(*(*_shredded_fields)[index].values);
}

const ColumnUInt8& ColumnVariantV2::ReadView::shredded_field_presence(size_t index) const {
    DORIS_CHECK_LT(index, shredded_field_count()) << "shredded field index is out of range";
    return *(*_shredded_fields)[index].presence;
}

size_t ColumnVariantV2::ReadView::residual_metadata_count() const noexcept {
    DORIS_CHECK(is_shredded()) << "residual_metadata_count requires shredded state";
    return _metadatas->size();
}

uint32_t ColumnVariantV2::ReadView::residual_metadata_id_at(size_t row) const {
    DORIS_CHECK(is_shredded()) << "residual_metadata_id_at requires shredded state";
    DORIS_CHECK_LT(row, size()) << "ColumnVariantV2 residual read row is out of range";
    const uint32_t id = _metadata_ids->get_data()[row];
    DORIS_CHECK_LT(id, residual_metadata_count())
            << "ColumnVariantV2 residual read metadata id is out of range";
    return id;
}

VariantMetadataRef ColumnVariantV2::ReadView::residual_metadata_at(uint32_t id) const {
    DORIS_CHECK(is_shredded()) << "residual_metadata_at requires shredded state";
    DORIS_CHECK_LT(id, residual_metadata_count())
            << "ColumnVariantV2 residual read metadata is out of range";
    const StringRef metadata = _metadatas->get_data_at(id);
    return {.data = metadata.data, .size = metadata.size};
}

VariantRef ColumnVariantV2::ReadView::residual_value_at(size_t row) const {
    const uint32_t id = residual_metadata_id_at(row);
    return {.metadata = residual_metadata_at(id), .value = _values->get_data_at(row)};
}

ColumnVariantV2::ReadView ColumnVariantV2::read_view() const {
    if (_typed) {
        DORIS_CHECK(_typed_type != nullptr) << "typed state requires a data type";
        return {static_cast<const IColumn::Ptr&>(_typed).get(), &_typed_type};
    }
    DORIS_CHECK(_typed_type == nullptr) << "non-typed state cannot retain a typed data type";
    if (!_shredded_fields.empty()) {
        return {_metadatas.get(), _meta_ids.get(), _values.get(), &_shredded_fields};
    }
    return {_metadatas.get(), _meta_ids.get(), _values.get()};
}

} // namespace doris
