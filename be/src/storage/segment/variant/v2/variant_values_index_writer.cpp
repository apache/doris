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
    for (size_t row = 0; row < length; ++row) {
        if (!outer_nulls.empty() && outer_nulls[row] != 0) {
            RETURN_IF_ERROR(_writer->add_nulls(1));
            continue;
        }
        const auto value = view.value_at(begin + row);
        RETURN_IF_ERROR(_writer->add_text_document([&value](const auto& consume) {
            return for_each_variant_leaf_text(value, consume);
        }));
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
