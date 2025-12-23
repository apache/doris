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

#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "vec/columns/column_variant.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

class VariantDocSnapshotIteratorBase : public ColumnIterator {
public:
    explicit VariantDocSnapshotIteratorBase(
            std::vector<BinaryColumnCacheSPtr>&& doc_value_column_caches);
    ~VariantDocSnapshotIteratorBase() override = default;

    Status init(const ColumnIteratorOptions& opts) override;
    Status seek_to_ordinal(ordinal_t ord) override;
    ordinal_t get_current_ordinal() const override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override = 0;
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override = 0;

protected:
    template <typename ReaderFunc>
    Status _collect_doc_snapshot_data(
            ReaderFunc&& reader_func,
            std::vector<vectorized::ColumnPtr>* doc_snapshot_data_buckets) {
        doc_snapshot_data_buckets->clear();
        doc_snapshot_data_buckets->reserve(_doc_value_column_caches.size());
        for (const auto& cache : _doc_value_column_caches) {
            DCHECK(cache);
            RETURN_IF_ERROR(reader_func(cache.get()));
            DCHECK(cache->binary_column);
            doc_snapshot_data_buckets->emplace_back(cache->binary_column->get_ptr());
        }
        return Status::OK();
    }

    std::vector<BinaryColumnCacheSPtr> _doc_value_column_caches;
};

class VariantDocSnapshotRootIterator final : public VariantDocSnapshotIteratorBase {
public:
    VariantDocSnapshotRootIterator(std::vector<BinaryColumnCacheSPtr>&& doc_value_column_caches,
                                   std::unique_ptr<SubstreamIterator>&& root_reader);

    Status init(const ColumnIteratorOptions& opts) override;
    Status seek_to_ordinal(ordinal_t ord) override;
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

private:
    Status _merge_doc_snapshot_into_variant(
            vectorized::MutableColumnPtr& dst,
            const std::vector<vectorized::ColumnPtr>& doc_snapshot_data_buckets,
            size_t num_rows) const;

    std::unique_ptr<SubstreamIterator> _root_reader;
};

class VariantDocSnapshotPathIterator final : public VariantDocSnapshotIteratorBase {
public:
    VariantDocSnapshotPathIterator(std::vector<BinaryColumnCacheSPtr>&& doc_value_column_caches,
                                   std::string path);

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

private:
    Status _merge_doc_snapshot_into_variant(
            vectorized::MutableColumnPtr& dst,
            const std::vector<vectorized::ColumnPtr>& doc_snapshot_data_buckets,
            size_t num_rows) const;

    std::string _path;
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2