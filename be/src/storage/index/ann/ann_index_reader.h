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

#include <cstdint>
#include <string>

#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_storage.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/index_common.h"
#include "storage/index/index_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"
namespace doris::segment_v2 {

struct AnnTopNParam;
struct AnnRangeSearchParams;
struct AnnRangeSearchResult;
struct IndexSearchResult;

class IndexFileReader;
class IndexIterator;
class AnnIndexCacheHandle;
struct AnnIndexCacheValue;

class AnnIndexReader : public IndexReader {
public:
    AnnIndexReader(const TabletIndex* index_meta,
                   std::shared_ptr<IndexFileReader> index_file_reader, std::string rowset_id = "",
                   uint32_t segment_id = 0, size_t rows_of_segment = 0);
    ~AnnIndexReader() override = default;

    // IVF_ON_DISK returns a query-scoped pin; the reader retains no loaded index.
    Status load_index(io::IOContext* io_ctx, AnnIndexCacheHandle* handle = nullptr);

    bool try_load_index(io::IOContext* io_ctx, AnnIndexCacheHandle* handle = nullptr);

    Status query(io::IOContext* io_ctx, AnnTopNParam* param, AnnIndexStats* stats,
                 AnnIndexCacheHandle* handle = nullptr);

    Status range_search(const AnnRangeSearchParams& params,
                        const VectorSearchUserParams& custom_params, AnnRangeSearchResult* result,
                        AnnIndexStats* stats, io::IOContext* io_ctx = nullptr,
                        AnnIndexCacheHandle* handle = nullptr);

    IndexType index_type() override { return IndexType::ANN; }

    uint64_t get_index_id() const override { return _index_meta.index_id(); }

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;

    AnnIndexMetric get_metric_type() const { return _metric_type; }

    size_t get_dimension() const;

private:
    Status _load_index(io::IOContext* io_ctx, AnnIndexCacheValue* value);

    TabletIndex _index_meta;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    // Non-disk indexes retain reader ownership. Disk IVF owns these in its cache
    // value instead. Source must outlive the inputs kept by the loaded index.
    std::unique_ptr<AnnIndexSource> _source;
    std::unique_ptr<VectorIndex> _vector_index;
    AnnIndexType _index_type;
    AnnIndexMetric _metric_type;
    size_t _dim;
    std::string _rowset_id;
    uint32_t _segment_id;
    size_t _rows_of_segment = 0;
    DorisCallOnce<Status> _load_index_once;
};

using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;
} // namespace doris::segment_v2
