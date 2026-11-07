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

#include "storage/segment/variant/v2/variant_values_index_writer.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/value/variant/variant_leaf_visitor.h"
#include "storage/index/inverted/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

VariantValuesIndexWriter::VariantValuesIndexWriter(IndexFileWriter* index_file_writer,
                                                   const TabletIndex* index_meta,
                                                   bool is_direct_load)
        : _index_file_writer(index_file_writer),
          _index_meta(index_meta),
          _is_direct_load(is_direct_load) {}

VariantValuesIndexWriter::~VariantValuesIndexWriter() {
    close_on_error();
}

Status VariantValuesIndexWriter::init() {
    DORIS_CHECK(_index_file_writer != nullptr);
    DORIS_CHECK(_index_meta != nullptr);
    DORIS_CHECK(_index_meta->is_variant_values_index());
    _writer = std::make_unique<SniiIndexColumnWriter>(_index_file_writer, _index_meta,
                                                      FieldType::OLAP_FIELD_TYPE_VARCHAR);
    RETURN_IF_ERROR(_writer->init());
    _writer->set_direct_load(_is_direct_load);
    return Status::OK();
}

Status VariantValuesIndexWriter::append(const ColumnVariantV2::ReadView& view, size_t begin,
                                        size_t length, std::span<const uint8_t> outer_nulls) {
    DORIS_CHECK(_writer != nullptr);
    DORIS_CHECK(!view.is_typed());
    DORIS_CHECK_LE(begin + length, view.size());
    DORIS_CHECK(outer_nulls.empty() || outer_nulls.size() == length);
    std::vector<Slice> values;
    for (size_t row = 0; row < length; ++row) {
        _texts.clear();
        values.clear();
        const uint8_t is_null = outer_nulls.empty() ? 0 : outer_nulls[row];
        if (!is_null) {
            RETURN_IF_ERROR(for_each_variant_leaf_text(view.value_at(begin + row),
                                                       [&](std::string_view text) {
                                                           _texts.emplace_back(text);
                                                           return Status::OK();
                                                       }));
        }
        // Canonical numeric text is callback-local. Take slices only after the owning vector
        // stopped growing, so moving short strings cannot invalidate their bytes.
        values.reserve(_texts.size());
        for (const auto& text : _texts) {
            values.emplace_back(text);
        }
        const uint64_t offsets[] = {0, values.size()};
        RETURN_IF_ERROR(_writer->add_array_values(sizeof(Slice),
                                                  reinterpret_cast<const uint8_t*>(values.data()),
                                                  nullptr, offsets, 1));
        RETURN_IF_ERROR(_writer->add_array_nulls(&is_null, 1));
    }
    return Status::OK();
}

Status VariantValuesIndexWriter::finish() const {
    DORIS_CHECK(_writer != nullptr);
    return _writer->finish();
}

void VariantValuesIndexWriter::close_on_error() {
    if (_writer != nullptr) {
        _writer->close_on_error();
    }
}

} // namespace doris::segment_v2
