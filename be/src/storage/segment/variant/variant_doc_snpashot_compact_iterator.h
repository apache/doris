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

#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/variant/v2/variant_assembler.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

class VariantDocValueCompactIterator : public ColumnIterator {
public:
    VariantDocValueCompactIterator(ColumnIteratorUPtr&& column_iterator, uint32_t path_encoding = 0)
            : _doc_value_iterator(std::move(column_iterator)), _path_encoding(path_encoding) {}

    Status init(const ColumnIteratorOptions& opts) override {
        return _doc_value_iterator->init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _doc_value_iterator->seek_to_ordinal(ord);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        MutableColumnPtr doc_value_column = ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(_doc_value_iterator->next_batch(n, doc_value_column, has_null));
        return _set_doc_value_into_variant(dst, std::move(doc_value_column), *n);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        MutableColumnPtr doc_value_column = ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(_doc_value_iterator->read_by_rowids(rowids, count, doc_value_column));
        return _set_doc_value_into_variant(dst, std::move(doc_value_column), count);
    }

    ordinal_t get_current_ordinal() const override {
        return _doc_value_iterator->get_current_ordinal();
    }

private:
    Status _set_doc_value_into_variant(MutableColumnPtr& dst, MutableColumnPtr&& doc_value_column,
                                       size_t count) const {
        if (check_and_get_column<ColumnVariantV2>(dst.get()) == nullptr) {
            if (_path_encoding != 0) {
                return Status::NotSupported("Escaped Variant storage requires a V2 destination");
            }
            auto& variant = assert_cast<ColumnVariant&>(*dst);
            auto container = ColumnVariant::create(variant.max_subcolumns_count(),
                                                   variant.enable_doc_mode(), count);
            container->set_doc_value_column(std::move(doc_value_column));
            variant.insert_range_from(*container, 0, count);
            return Status::OK();
        }
        auto* storage_map = check_and_get_column<ColumnMap>(doc_value_column.get());
        if (storage_map == nullptr) {
            return Status::Corruption("Variant doc compact input is not Map<String,String>");
        }
        variant_v2::VariantAssemblerOptions options;
        options.path_encoding = _path_encoding;
        options.storage_map_kind = variant_v2::StorageMapKind::DOC;
        auto assembler = DORIS_TRY(variant_v2::VariantAssembler::create(std::move(options)));
        variant_v2::VariantAssemblerBatchView batch;
        batch.num_rows = count;
        batch.storage_map = storage_map;
        ColumnNullable::MutablePtr assembled;
        RETURN_IF_ERROR(assembler->assemble(batch, &assembled));
        return variant_v2::append_assembled_variant(dst, std::move(assembled));
    }

    ColumnIteratorUPtr _doc_value_iterator;
    uint32_t _path_encoding;
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
