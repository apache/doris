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

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column_variant.h"

class VariantDocValueCompactIterator : public ColumnIterator {
public:
    VariantDocValueCompactIterator(ColumnIteratorUPtr&& column_iterator)
            : _doc_value_iterator(std::move(column_iterator)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        return _doc_value_iterator->init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _doc_value_iterator->seek_to_ordinal(ord);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        vectorized::MutableColumnPtr doc_value_column =
                vectorized::ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(_doc_value_iterator->next_batch(n, doc_value_column, has_null));
        return _set_doc_value_into_variant(dst, std::move(doc_value_column), *n);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        vectorized::MutableColumnPtr doc_value_column =
                vectorized::ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(_doc_value_iterator->read_by_rowids(rowids, count, doc_value_column));
        return _set_doc_value_into_variant(dst, std::move(doc_value_column), count);
    }

    ordinal_t get_current_ordinal() const override {
        return _doc_value_iterator->get_current_ordinal();
    }

private:
    Status _set_doc_value_into_variant(vectorized::MutableColumnPtr& dst,
                                       vectorized::MutableColumnPtr&& doc_value_column,
                                       size_t count) const {
        auto& variant = assert_cast<vectorized::ColumnVariant&>(*dst);
        vectorized::MutableColumnPtr container =
                vectorized::ColumnVariant::create(variant.max_subcolumns_count(), count);
        auto& container_variant = assert_cast<vectorized::ColumnVariant&>(*container);
        container_variant.set_doc_value_column(std::move(doc_value_column));
        variant.insert_range_from(container_variant, 0, count);
        return Status::OK();
    }

    ColumnIteratorUPtr _doc_value_iterator;
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2